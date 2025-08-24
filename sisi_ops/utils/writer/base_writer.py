from abc import ABC, abstractmethod


class BaseWriter(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def insert(self) -> None:
        raise TypeError(
            "BaseWriter has not functionality for insert"
        )

    @abstractmethod
    def update(self, ) -> None:
        raise TypeError(
            "BaseWriter has not functionality for update"
        )

    @abstractmethod
    def upsert(self) -> None:
        raise TypeError(
            "BaseWriter has not functionality for upsert"
        )
