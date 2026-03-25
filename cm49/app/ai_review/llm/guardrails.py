from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .budget_manager import BudgetManager
from .cache_manager import LLMCacheManager


@dataclass
class GuardrailDecision:
    allowed: bool
    reason: str | None = None
    cached_response: dict[str, Any] | None = None


class LLMGuardrails:
    def __init__(self, budget_manager: BudgetManager, cache_manager: LLMCacheManager, prompt_version: str):
        self.budget_manager = budget_manager
        self.cache_manager = cache_manager
        self.prompt_version = prompt_version

    def check(self, row_key: str, capability: str, context: dict[str, Any], estimated_cost_eur: float, *, provider: str = '', model: str = '', prompt_text: str = '') -> GuardrailDecision:
        cached = self.cache_manager.get(capability, context, self.prompt_version, provider=provider, model=model, prompt_text=prompt_text)
        if cached is not None:
            return GuardrailDecision(allowed=False, reason='cache_hit', cached_response=cached)
        ok, reason = self.budget_manager.can_spend(row_key=row_key, estimated_cost_eur=estimated_cost_eur)
        if not ok:
            return GuardrailDecision(allowed=False, reason=reason)
        return GuardrailDecision(allowed=True)
