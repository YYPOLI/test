# PermitGuard

**An Interpretable Neuro-Symbolic Framework for Ethereum Permit Phishing Detection**

PermitGuard is a neuro-symbolic detection framework that identifies ERC-20 permit phishing attacks by verifying *semantic consistency* between user authorization intent and on-chain execution outcomes. It combines deterministic invariant constraints with LLM-based cognitive reasoning to achieve high-accuracy, interpretable detection.

## Architecture

PermitGuard comprises three core modules:

```
┌──────────────────────────────────────────────────────────┐
│                     PermitGuard                          │
├──────────────┬──────────────────┬────────────────────────┤
│ Permit Parser│ Semantic Aligner │ Cognitive Reasoner     │
│ (Data Layer) │ (Constraint Layer)│ (Reasoning Layer)     │
│              │                  │                        │
│ • Payload    │ • Submitter      │ • State-Machine        │
│   Decoder    │   Profiling      │   Controlled Prompt    │
│ • Context    │ • Intent         │ • Double-Blind         │
│   Retriever  │   Quantification │   Inference            │
│ • Trace      │ • State          │ • Risk Verdict         │
│   Fetcher    │   Transition     │   Generation           │
│              │   Verification   │                        │
└──────────────┴──────────────────┴────────────────────────┘
```

## Project Structure

```
PermitGuard/
├── src/                           # Core detection framework
│   ├── utils/
│   │   ├── config.py              # Centralized configuration & API key management
│   │   └── hex_parser.py          # On-chain data decoder (permit/transferFrom)
│   ├── permit_parser/             # Module 1: Permit Parser
│   │   ├── context_retriever.py   # KnowledgeBase: tokens, labels, entity profiles
│   │   ├── trace_fetcher.py       # ForensicsAnalyzer: ghost detection & delayed drain
│   │   └── payload_decoder.py     # DeepFactEnricher: raw tx → enriched fact sheet
│   └── cognitive_reasoner/        # Module 2 & 3: Semantic Aligner + Cognitive Reasoner
│       ├── constrained_inferencer.py  # SemanticAligner + PermitGuardAuditor
│       └── prompts/
│           └── audit_prompt.txt   # Externalized LLM prompt template
│
├── data/                          # Data collection & preprocessing pipeline
│   ├── 01_bigquery_extraction/    # BigQuery SQL (TODO: add .sql files)
│   ├── 02_data_processing/        # JSONL cleaning, deduplication, parsing
│   │   └── data_processor.py
│   ├── 03_label_collection/       # Etherscan nametag crawling
│   │   └── etherscan_crawler.py
│   └── 04_feature_engineering/    # Entity-level feature extraction
│       └── feature_extractor.py
│
├── evaluation/                    # Detection pipeline & metrics
│   ├── run_detection.py           # End-to-end detection entry point
│   └── calculate_metrics.py       # Post-hoc F1/Precision/Recall/FPR calculation
│
├── visualization/                 # Paper figure generation
│   ├── plot_figures.py            # All plotting functions
│   └── figures/                   # Output directory for generated figures
│
├── sample/                        # Sample data for reproducibility (TODO)
├── .env.example                   # API key template
├── .gitignore
├── requirements.txt
└── README.md
```

## Setup

1. Clone the repository:
```bash
git clone https://github.com/YOUR_USERNAME/PermitGuard.git
cd PermitGuard
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure API keys:
```bash
cp .env.example .env
# Edit .env with your actual API keys
```

## Usage

### Run Detection
```bash
python evaluation/run_detection.py
```

### Calculate Metrics from Saved Report
```bash
python evaluation/calculate_metrics.py
```

### Generate Paper Figures
```bash
python visualization/plot_figures.py monthly_permits
python visualization/plot_figures.py combined_features_dumbbell
```

## Data Pipeline

The data processing pipeline follows four stages:

1. **BigQuery Extraction** — SQL queries to extract permit execution traces from Ethereum
2. **Data Processing** — Clean, deduplicate, and parse raw JSONL exports
3. **Label Collection** — Crawl Etherscan for address nametags and assign labels
4. **Feature Engineering** — Extract behavioral features for Spender/Submitter entities

## Citation

If you use PermitGuard in your research, please cite:

```bibtex
@article{permitguard2026,
  title={PermitGuard: An Interpretable Neuro-Symbolic Framework for Ethereum Permit Phishing Detection},
  year={2026}
}
```

## License

This project is for academic research purposes.
