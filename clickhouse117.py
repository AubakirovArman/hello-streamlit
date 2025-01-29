import logging
from typing import List, Union, Generator, Iterator
from openwebui.pipeline import Pipeline, register_pipeline
from llama_index.llms.ollama import Ollama
from llama_index.core import PromptTemplate

logging.basicConfig(level=logging.DEBUG)

class Llama3Pipeline(Pipeline):
    def __init__(self):
        """Инициализация LLM"""
        self.name = "Llama3 Query Generator"
        
        # Подключаем Ollama с Llama 3
        self.llm = Ollama(
            model="llama3:latest",
            base_url="http://100.119.234.241:11434"  # Укажите свой сервер Ollama
        )

        # Простой шаблон промпта для генерации SQL-запросов
        self.text_to_sql_prompt = PromptTemplate("""
        You are an AI that converts user questions into SQL queries.
        Given a question, generate a valid SQL SELECT query.

        Question: {query_str}
        SQLQuery:
        """)

    def run(self, message: str, user_id: str, chat_history: List[str]) -> str:
        """
        Обрабатывает текстовый запрос и генерирует SQL-запрос с помощью LLM.
        """
        try:
            # Создаем запрос к LLM
            query = self.text_to_sql_prompt.format(query_str=message)
            response = self.llm.complete(query)

            # Возвращаем сгенерированный SQL-запрос
            return f"Generated SQL Query:\n```sql\n{response}\n```"
        
        except Exception as e:
            logging.error(f"⛔ Error: {e}")
            return f"❌ Error: {e}"

# Регистрируем пайплайн в OpenWebUI
register_pipeline("llama3_pipeline", Llama3Pipeline)
