"""
LangGraph Human-in-the-loop: custom interrupt / resume

Реализует граф с одним узлом, который прерывает выполнение для запроса
подтверждения у пользователя через консольное меню (questionary).
После выбора ответа граф возобновляется и сохраняет результат в состояние.
"""

from __future__ import annotations

import questionary
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.constants import START
from langgraph.graph import StateGraph
from langgraph.types import Command, interrupt
from typing_extensions import TypedDict


# ---------------------------------------------------------------------------
# 1. Состояние графа
# ---------------------------------------------------------------------------
class State(TypedDict):
    """Состояние графа с полем для значения, введённого пользователем."""

    human_value: str | None
    foo: str | None


# ---------------------------------------------------------------------------
# 2. Узел с прерыванием
# ---------------------------------------------------------------------------
def approval_node(state: State) -> dict:
    """
    Узел, который запрашивает подтверждение у пользователя.

    Вызывает interrupt() со словарём, содержащим тип, вопрос и варианты ответа.
    После возобновления (resume) payload возвращается в узел — мы извлекаем
    ответ пользователя и сохраняем его в human_value.
    """
    # Если human_value уже заполнен (узел перезапускается при resume),
    # просто возвращаем текущее состояние без повторного прерывания.
    if state.get("human_value") is not None:
        return {}

    payload = interrupt(
        {
            "type": "alert",
            "question": "Уверены что хотите продолжить?",
            "allow_responds": ["approve", "reject"],
        }
    )

    # После resume payload содержит обновлённый объект с полем "answer"
    answer = payload.get("answer")
    return {"human_value": answer}


# ---------------------------------------------------------------------------
# 3. Сборка графа
# ---------------------------------------------------------------------------
def build_graph():
    """Создаёт и компилирует граф с InMemorySaver."""

    builder = StateGraph(State)
    builder.add_node("approval", approval_node)
    builder.add_edge(START, "approval")

    # Компиляция с чекпоинтером для поддержки interrupt/resume
    checkpointer = InMemorySaver()
    graph = builder.compile(checkpointer=checkpointer)
    return graph


# ---------------------------------------------------------------------------
# 4. Цикл запуска с обработкой прерывания
# ---------------------------------------------------------------------------
def run_graph(graph):
    """
    Запускает граф через stream(), обрабатывает прерывание,
    получает ответ пользователя и возобновляет выполнение.
    """

    config = {"configurable": {"thread_id": "thread-1"}}
    initial_input: dict = {"foo": "initial_data"}

    # --- Первый запуск: до прерывания ---
    interrupted = False
    interrupt_payload: dict | None = None

    for chunk in graph.stream(initial_input, config=config):
        # Проверяем, есть ли прерывание в чанке
        if "__interrupt__" in chunk:
            interrupted = True
            # Извлекаем payload из первого объекта Interrupt
            interrupt_payload = chunk["__interrupt__"][0].value
            print("Произошла остановка")
            print(interrupt_payload)
            break  # Выходим из цикла — ждём ответа пользователя

    if not interrupted or interrupt_payload is None:
        print("Ошибка: прерывание не произошло.")
        return

    # --- Показываем вопрос пользователю ---
    print(f"!!! {interrupt_payload['type']} !!!")
    question = interrupt_payload["question"]
    choices = interrupt_payload["allow_responds"]

    answer = questionary.select(question, choices=choices).ask()

    print(f"> Received an input from the interrupt: {answer}")

    # --- Обновляем payload и возобновляем граф ---
    interrupt_payload["answer"] = answer

    for chunk in graph.stream(Command(resume=interrupt_payload), config=config):
        # Выводим обновления состояния после возобновления
        if "__interrupt__" not in chunk:
            print(chunk)

    # --- Вывод итогового состояния ---
    final_state = graph.get_state(config).values
    print(f"Итоговое состояние: {final_state}")


# ---------------------------------------------------------------------------
# Точка входа
# ---------------------------------------------------------------------------
def main() -> None:
    """Основная функция: создаёт граф и запускает его."""
    graph = build_graph()
    run_graph(graph)


if __name__ == "__main__":
    main()
