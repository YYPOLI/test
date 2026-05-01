import json
import copy
import os
from typing import Dict, Any

from google import genai

from src.utils.config import CONFIG


PROMPT_PATH = os.path.join(os.path.dirname(__file__), "prompts", "audit_prompt.txt")

with open(PROMPT_PATH, "r", encoding="utf-8") as _f:
    _PROMPT_TEMPLATE = _f.read()


class SemanticAligner:
    """
    Computes cross-stage semantic invariant constraints (Submitter Profiling,
    Intent Quantification, State Transition Verification) and injects them as
    a statistical risk summary into the fact sheet for the LLM.
    """

    @staticmethod
    def mask_labels(safe_facts: dict) -> dict:
        """
        Generate desensitized state O_mask = MASK(O) (§5.4).
        Masks malicious labels (Label 1 / Phishing) to prevent LLM from taking
        shortcuts based on known blacklist hits. Benign labels (Label 2) from
        the address label library are retained as part of the external knowledge
        K provided by the Context Retriever, serving as safe-harbor anchors for
        downstream constraint computation.
        """
        entities = safe_facts.get('entities', {})
        exec_info = safe_facts.get('execution_forensics', {})

        for info, label_key, nametag_key in [
            (entities.get('spender', {}), 'label_spender', 'nametag_spender'),
            (entities.get('submitter', {}), 'label_submitter', 'nametag_submitter'),
            (entities.get('relayer', {}), 'label_relayer', 'nametag_relayer'),
            (exec_info, 'label_transfer_to', 'nametag_transfer_to'),
        ]:
            original = info.get(label_key, 0)
            if original == 1 or str(original).lower() == 'phishing':
                info[label_key] = 0
                info[nametag_key] = 'Unknown'
            elif original == 2 or str(original).lower() == 'benign':
                info[label_key] = 2
            else:
                info[label_key] = 0
                info[nametag_key] = 'Unknown'

        return safe_facts

    @staticmethod
    def compute_constraints(safe_facts: dict) -> dict:
        """Derive the cross-stage semantic constraint matrix O from the enriched facts."""
        entities = safe_facts.get('entities', {})
        sp_info = entities.get('spender', {})
        sb_info = entities.get('submitter', {})
        relayer_info = entities.get('relayer', {})
        exec_info = safe_facts.get('execution_forensics', {})
        intent_info = safe_facts.get('permit_intent', {})

        addr_submitter = sb_info.get('address', '').lower()
        addr_relayer = relayer_info.get('address', '').lower()
        addr_owner = entities.get('owner_address', '').lower()
        addr_spender = sp_info.get('address', '').lower()
        addr_token = intent_info.get('token_address', '').lower()
        addr_transfer_to = exec_info.get('transfer_to', '').lower()
        token_symbol = intent_info.get('token_symbol', 'Unknown')

        # ===== Phase 1: Submitter Profiling (Algorithm 1, Lines 4-7) =====
        # V_sub = <Freq_tx, Freq_LP, N_owners>
        Freq_tx = sb_info.get('total_txs', 0)
        Freq_LP = sb_info.get('feat_lp_token', 0)
        N_owners = sb_info.get('N_owners', 0)
        sb_ratio_mediated = sb_info.get('ratio_mediated', 0.5)
        sb_high_value_rate = sb_info.get('sb_high_value_ratio', 0.6)
        unique_lp_tokens = sb_info.get('unique_lp_tokens', 0)

        is_high_freq_porter = (Freq_tx > 10) or (N_owners > 5)
        is_self_submit = (sb_info.get('relationship_to_owner') == 'Self (Owner)')
        is_malicious_sweeping = (Freq_LP > 1.5) and (sb_info.get('label_submitter') != 2)

        # ===== Phase 2: Intent Quantification (Algorithm 1, Lines 8-10) =====
        # GhostSpender, ΔT, E_risk
        GhostSpender = sp_info.get('is_ghost', False)

        intent_risk = intent_info.get('risk_flags', {})
        is_infinite_time = intent_risk.get('is_infinite_time', False)
        is_infinite_amount = intent_risk.get('is_infinite_amount', False)
        is_junk_asset = (token_symbol == 'Unknown')

        rho = exec_info.get('permit_transfer_ratio', 0.0)
        if isinstance(rho, str) and '%' in rho:
            try:
                rho = float(rho.strip('%')) / 100.0
            except Exception:
                rho = 0.0

        is_harvesting_signal = is_infinite_amount and (rho < 0.001)

        # ===== Phase 3: State Transition Verification (Algorithm 1, Lines 11-15) =====
        # V_exp = {Owner, Spender, Submitter, Relayer, TokenContract}
        V_exp = {addr_owner, addr_spender, addr_submitter, addr_relayer, addr_token}
        TopologyLeak = (addr_transfer_to not in V_exp) and (addr_transfer_to != '')

        is_solver_settlement = (addr_transfer_to == addr_relayer) or (addr_transfer_to == addr_submitter)

        tf_label = exec_info.get('label_transfer_to', 0)
        TopologyLeak_risk = TopologyLeak and (tf_label != 2)

        # ===== Phase 4: Cross-stage Semantic Constraint Ψ (Algorithm 1, Line 16) =====
        # O = Ψ(V_sub, GhostSpender, ΔT, E_risk, ρ, TopologyLeak)
        combo_dormant = is_infinite_time and GhostSpender
        combo_relayed_theft = (not is_self_submit) and TopologyLeak_risk
        combo_self_routed = is_self_submit and TopologyLeak_risk

        return {
            "Phase1_SubmitterProfiling": {
                "is_malicious_sweeping": is_malicious_sweeping,
                "is_self_submit": is_self_submit,
                "is_high_freq_porter": is_high_freq_porter,
                "V_sub": {
                    "Freq_tx": Freq_tx,
                    "Freq_LP": Freq_LP,
                    "N_owners": N_owners,
                    "unique_lp_tokens": unique_lp_tokens,
                    "handling_rate": sb_ratio_mediated,
                    "high_value_rate": sb_high_value_rate,
                },
            },
            "Phase2_IntentQuantification": {
                "GhostSpender": GhostSpender,
                "is_infinite_amount": is_infinite_amount,
                "is_infinite_time": is_infinite_time,
                "is_junk_asset": is_junk_asset,
                "is_harvesting_signal": is_harvesting_signal,
                "details": {
                    "validity_days": intent_info.get('validity_period'),
                    "rho": rho,
                },
            },
            "Phase3_StateTransition": {
                "is_solver_settlement": is_solver_settlement,
                "TopologyLeak": TopologyLeak_risk,
            },
            "Phase4_CrossStageConstraint": {
                "combo_dormant_trap": combo_dormant,
                "combo_relayed_theft": combo_relayed_theft,
                "combo_self_routed": combo_self_routed,
            },
        }


class ConstrainedInferencer:
    """
    Constrained Inferencer (§5.4): guides LLM to perform zero-shot detection
    under deterministic factual constraints through state-machine-controlled
    reasoning and evidence stacking paradigm (Eq. 3).
    """

    def __init__(self):
        self.client = genai.Client(api_key=CONFIG["GEMINI_API_KEY"])
        self.model_name = CONFIG["MODEL_NAME"]

    def audit(self, enriched_facts: Dict[str, Any]) -> Dict[str, Any]:
        safe_facts = copy.deepcopy(enriched_facts)

        # Phase 1: Label masking (double-blind)
        safe_facts = SemanticAligner.mask_labels(safe_facts)

        # Phase 2: Compute semantic constraints
        statistical_summary = SemanticAligner.compute_constraints(safe_facts)
        safe_facts['STATISTICAL_RISK_SUMMARY'] = statistical_summary

        # Phase 3: LLM inference
        facts_json = json.dumps(safe_facts, indent=2)
        prompt = _PROMPT_TEMPLATE.format(facts_json=facts_json)

        try:
            response = self.client.models.generate_content(
                model=self.model_name,
                contents=prompt,
                config={'response_mime_type': 'application/json'},
            )

            content = ""
            if response.candidates and response.candidates[0].content.parts:
                for part in response.candidates[0].content.parts:
                    if hasattr(part, 'text') and part.text:
                        content = part.text
                        break
            if not content:
                content = response.text

            usage = getattr(response, 'usage_metadata', None)
            prompt_tokens = usage.prompt_token_count if usage else 0
            completion_tokens = usage.candidates_token_count if usage else 0

            result_dict = json.loads(content)
            result_dict['usage'] = {
                'prompt_tokens': prompt_tokens,
                'completion_tokens': completion_tokens,
            }
            return result_dict

        except Exception as e:
            return {
                "risk_level": "UNKNOWN",
                "primary_reason": f"Error: {str(e)}",
                "usage": {"prompt_tokens": 0, "completion_tokens": 0},
            }
