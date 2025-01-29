import clickhouse_connect
import logging
from typing import List, Union, Generator, Iterator
import os
from pydantic import BaseModel
import aiohttp
import asyncio

logging.basicConfig(level=logging.DEBUG)

class Pipeline:
    class Valves(BaseModel):
        DB_HOST: str
        DB_PORT: str
        DB_USER: str
        DB_PASSWORD: str
        DB_DATABASE: str

    def __init__(self):
        self.name = "ClickHouse Database Query"
        self.client = None  # Соединение с ClickHouse

        self.valves = self.Valves(
            **{
                "DB_HOST": os.getenv("CH_HOST", "195.12.114.20"),
                "DB_PORT": os.getenv("CH_PORT", "8123"),
                "DB_USER": os.getenv("CH_USER", "default"),
                "DB_PASSWORD": os.getenv("CH_PASSWORD", "Nitec123"),
                "DB_DATABASE": os.getenv("CH_DB", "FR"),
            }
        )

    def init_db_connection(self):
        """
        Устанавливает соединение с ClickHouse.
        """
        try:
            self.client = clickhouse_connect.get_client(
                host=self.valves.DB_HOST,
                port=int(self.valves.DB_PORT),
                username=self.valves.DB_USER,
                password=self.valves.DB_PASSWORD,
                database=self.valves.DB_DATABASE
            )
            logging.info("✅ Connection to ClickHouse established successfully")
        except Exception as e:
            logging.error(f"⛔ Error connecting to ClickHouse: {e}")

    async def on_startup(self):
        """
        Функция вызывается при старте сервера OpenWebUI.
        """
        self.init_db_connection()

    async def on_shutdown(self):
        """
        Функция вызывается при завершении работы сервера OpenWebUI.
        """
        if self.client:
            self.client.close()
            logging.info("🔌 ClickHouse connection closed.")

    def execute_query(self, query: str):
        """
        Выполняет SQL-запрос в ClickHouse и возвращает результат.
        """
        try:
            result = self.client.query(query)
            return str(result.result_rows)  # Преобразуем в строку
        except Exception as e:
            logging.error(f"⛔ Query execution error: {e}")
            return f"❌ Query execution error: {e}"

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        """
        Обрабатывает запрос пользователя и выполняет SQL-запрос в ClickHouse.
        """
        try:
            query = user_message.strip()  # Убираем лишние пробелы
            if not query.lower().startswith(("select", "show", "describe")):
                return "⛔ Only SELECT, SHOW, or DESCRIBE queries are allowed!"

            result = self.execute_query(query)
            return result

        except Exception as e:
            logging.error(f"⛔ Unexpected error: {e}")
            return f"❌ Unexpected error: {e}"
