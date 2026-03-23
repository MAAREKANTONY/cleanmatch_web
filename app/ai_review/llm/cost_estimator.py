from __future__ import annotations


class CostEstimator:
    def __init__(self, input_price_per_1k_tokens_eur: float, output_price_per_1k_tokens_eur: float):
        self.input_price_per_1k_tokens_eur = float(input_price_per_1k_tokens_eur or 0.0)
        self.output_price_per_1k_tokens_eur = float(output_price_per_1k_tokens_eur or 0.0)

    @staticmethod
    def rough_token_estimate(text: str) -> int:
        text = str(text or '')
        if not text:
            return 0
        return max(1, len(text) // 4)

    def estimate(self, prompt_text: str, max_completion_tokens: int) -> tuple[float, int, int]:
        prompt_tokens = self.rough_token_estimate(prompt_text)
        completion_tokens = int(max_completion_tokens or 0)
        return self.estimate_from_token_counts(prompt_tokens, completion_tokens)

    def estimate_from_token_counts(self, prompt_tokens: int, completion_tokens: int) -> tuple[float, int, int]:
        cost = ((int(prompt_tokens) / 1000.0) * self.input_price_per_1k_tokens_eur) + ((int(completion_tokens) / 1000.0) * self.output_price_per_1k_tokens_eur)
        return float(cost), int(prompt_tokens), int(completion_tokens)
