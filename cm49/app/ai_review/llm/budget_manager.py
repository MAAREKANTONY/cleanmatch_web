from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class BudgetManager:
    max_budget_eur: float
    max_cost_per_row_eur: float
    max_calls_per_row: int
    current_spend_eur: float = 0.0
    row_spend: dict[str, float] = field(default_factory=dict)
    row_calls: dict[str, int] = field(default_factory=dict)

    def can_spend(self, row_key: str, estimated_cost_eur: float) -> tuple[bool, str | None]:
        if estimated_cost_eur <= 0:
            return True, None
        if self.current_spend_eur + estimated_cost_eur > self.max_budget_eur:
            return False, 'budget_exceeded'
        if self.row_spend.get(row_key, 0.0) + estimated_cost_eur > self.max_cost_per_row_eur:
            return False, 'row_budget_exceeded'
        if self.row_calls.get(row_key, 0) >= self.max_calls_per_row:
            return False, 'max_calls_per_row_exceeded'
        return True, None

    def register_cost(self, row_key: str, cost_eur: float) -> None:
        cost_eur = max(float(cost_eur or 0.0), 0.0)
        self.current_spend_eur += cost_eur
        self.row_spend[row_key] = self.row_spend.get(row_key, 0.0) + cost_eur
        self.row_calls[row_key] = self.row_calls.get(row_key, 0) + 1

    def remaining_budget(self) -> float:
        return max(self.max_budget_eur - self.current_spend_eur, 0.0)
