# 🚀 Airflow E-Commerce Data Pipeline

[![CI](https://github.com/louisbentz2002-crypto/airflow-ecommerce-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/louisbentz2002-crypto/airflow-ecommerce-pipeline/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Airflow 2.x](https://img.shields.io/badge/Airflow-2.x-017CEE.svg)](https://airflow.apache.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Un pipeline ELT complet orchestré par **Apache Airflow** pour un dataset e-commerce fictif. Ce projet démontre les bonnes pratiques de data engineering : orchestration, data quality, idempotence, Docker, CI/CD.


---

## 📋 Table des matières

- [Architecture](#-architecture)
- [Dataset](#-dataset)
- [Setup en 5 minutes](#-setup-en-5-minutes)
- [DAGs](#-dags)
- [Structure du projet](#-structure-du-projet)
- [Data Quality](#-data-quality)
- [Tests](#-tests)
- [Compétences démontrées](#-compétences-démontrées)
- [Roadmap](#-roadmap)

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           AIRFLOW ORCHESTRATION                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐  │
│   │   Extract   │───▶│  Load Raw   │───▶│  Transform  │───▶│  Analytics  │  │
│   │  (CSV/Gen)  │    │  (Postgres) │    │  (Staging)  │    │   (Marts)   │  │
│   └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘  │
│         │                  │                  │                  │          │
│         ▼                  ▼                  ▼                  ▼          │
│   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐  │
│   │  Validate   │    │  DQ Checks  │    │  SQL Clean  │    │  DQ Checks  │  │
│   │   Schema    │    │   (raw.*)   │    │  Dedupe     │    │  (marts.*)  │  │
│   └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                              POSTGRES LAYERS                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ┌───────────────────┐  ┌───────────────────┐  ┌───────────────────────┐   │
│   │      raw.*        │  │    staging.*      │  │       mart.*          │   │
│   │                   │  │                   │  │                       │   │
│   │  - raw_orders     │  │  - stg_orders     │  │  - revenue_daily      │   │
│   │  - raw_customers  │  │  - stg_customers  │  │  - top_products       │   │
│   │  - raw_products   │  │  - stg_products   │  │  - customer_ltv       │   │
│   │  - raw_payments   │  │  - stg_payments   │  │                       │   │
│   └───────────────────┘  └───────────────────┘  └───────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                           DOCKER COMPOSE STACK                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐  │
│   │  Airflow    │    │  Airflow    │    │  Postgres   │    │  PgAdmin    │  │
│   │  Webserver  │    │  Scheduler  │    │    (DWH)    │    │ (optional)  │  │
│   │   :8080     │    │             │    │    :5432    │    │   :5050     │  │
│   └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 📊 Dataset

Dataset e-commerce fictif généré avec **Faker** :

| Table | Description | Volume |
|-------|-------------|--------|
| `orders` | Commandes clients | ~10k lignes/jour |
| `customers` | Informations clients | ~1k lignes |
| `products` | Catalogue produits | ~500 lignes |
| `payments` | Transactions | ~10k lignes/jour |

**Génération automatique** via `src/data_generator.py` (pas de dépendance externe).

---

## ⚡ Setup en 5 minutes

### 🎯 Option 1 : Demo sans installation (Python seul)

La méthode la plus simple pour tester le projet :

```bash
git clone https://github.com/YOUR_USERNAME/airflow-ecommerce-pipeline.git
cd airflow-ecommerce-pipeline

# Exécuter le pipeline et générer les données
python run_standalone_demo.py

# Lancer le dashboard web
python run_dashboard.py
```

Ouvrir **http://localhost:8080** pour voir les analytics.

---

### 🐳 Option 2 : Dashboard avec Docker

```bash
# Build et run en une commande
docker-compose -f docker-compose.dashboard.yml up --build

# Ou build manuel
docker build -f Dockerfile.dashboard -t ecommerce-dashboard .
docker run -p 8080:8080 ecommerce-dashboard
```

---

### 🚀 Option 3 : Stack complet Airflow + PostgreSQL

#### Prérequis

- Docker & Docker Compose v2+
- Python 3.11+ (pour les tests locaux)
- Git

#### 1. Cloner le repo

```bash
git clone https://github.com/louisbentz2002-crypto/airflow-ecommerce-pipeline.git
cd airflow-ecommerce-pipeline
```

### 2. Configurer l'environnement

```bash
# Copier le fichier d'environnement
cp .env.example .env

# (Optionnel) Modifier les variables dans .env
```

### 3. Lancer le stack

```bash
# Construire et démarrer les services
docker-compose up -d --build

# Vérifier que tout est lancé
docker-compose ps
```

### 4. Accéder à Airflow

- **URL** : http://localhost:8080
- **Login** : `airflow` / `airflow`

### 5. Configurer les connexions Airflow

Dans l'interface Airflow (**Admin > Connections**), créer :

| Conn Id | Conn Type | Host | Port | Schema | Login | Password |
|---------|-----------|------|------|--------|-------|----------|
| `postgres_dwh` | Postgres | `postgres` | `5432` | `ecommerce` | `airflow` | `airflow` |

Ou via CLI :

```bash
docker-compose exec airflow-webserver airflow connections add 'postgres_dwh' \
    --conn-type 'postgres' \
    --conn-host 'postgres' \
    --conn-port '5432' \
    --conn-schema 'ecommerce' \
    --conn-login 'airflow' \
    --conn-password 'airflow'
```

### 6. Activer et lancer les DAGs

1. Dans l'UI Airflow, activer les DAGs :
   - `dag_ingest_ecommerce`
   - `dag_transform_ecommerce`
   - `dag_daily_end_to_end`

2. Déclencher `dag_daily_end_to_end` manuellement ou attendre l'exécution planifiée.

### Commandes utiles

```bash
# Voir les logs
docker-compose logs -f airflow-scheduler

# Arrêter le stack
docker-compose down

# Reset complet (supprime les volumes)
docker-compose down -v

# Générer des données de test
docker-compose exec airflow-scheduler python /opt/airflow/src/data_generator.py

# Lancer les tests
docker-compose exec airflow-scheduler pytest /opt/airflow/tests -v
```

---

## 📅 DAGs

### 1. `dag_ingest_ecommerce` (Ingestion)

Charge les données brutes depuis CSV ou génère des données fictives.

```
┌──────────────────────────────────────────────────────────────┐
│                    dag_ingest_ecommerce                      │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  [TaskGroup: extract]                                        │
│     │                                                        │
│     ├── detect_new_files                                     │
│     ├── validate_schema                                      │
│     └── generate_data (if no files)                          │
│                │                                             │
│                ▼                                             │
│         load_raw_postgres                                    │
│                │                                             │
│                ▼                                             │
│          dq_raw_checks                                       │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

**Schedule** : `@daily`  
**Retry** : 3 fois, backoff exponentiel  
**SLA** : 30 minutes

### 2. `dag_transform_ecommerce` (Transformation)

Exécute les transformations SQL (staging → mart).

```
┌──────────────────────────────────────────────────────────────┐
│                  dag_transform_ecommerce                     │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│         run_sql_staging                                      │
│         (staging_orders, staging_customers, ...)             │
│                │                                             │
│                ▼                                             │
│          run_sql_mart                                        │
│         (mart_revenue_daily, mart_top_products, ...)         │
│                │                                             │
│                ▼                                             │
│         dq_mart_checks                                       │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

**Schedule** : None (triggered)  
**Idempotence** : TRUNCATE + INSERT ou UPSERT avec partition date

### 3. `dag_daily_end_to_end` (Orchestration)

Pipeline complet quotidien.

```
┌──────────────────────────────────────────────────────────────┐
│                   dag_daily_end_to_end                       │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│         trigger_ingest                                       │
│                │                                             │
│                ▼                                             │
│        wait_for_ingest                                       │
│                │                                             │
│                ▼                                             │
│       trigger_transform                                      │
│                │                                             │
│                ▼                                             │
│       wait_for_transform                                     │
│                │                                             │
│                ▼                                             │
│          end_pipeline                                        │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

**Schedule** : `0 6 * * *` (6h UTC chaque jour)

---

## 📁 Structure du projet

```
airflow-ecommerce-pipeline/
├── .github/
│   └── workflows/
│       └── ci.yml                 # GitHub Actions CI
├── dags/
│   ├── dag_ingest_ecommerce.py    # DAG ingestion
│   ├── dag_transform_ecommerce.py # DAG transformation
│   └── dag_daily_end_to_end.py    # DAG orchestration
├── data/
│   └── incoming/                  # CSV source (simule S3)
├── sql/
│   ├── init/
│   │   └── init_schemas.sql       # Création schémas Postgres
│   ├── staging/
│   │   ├── staging_orders.sql
│   │   ├── staging_customers.sql
│   │   ├── staging_products.sql
│   │   └── staging_payments.sql
│   └── mart/
│       ├── mart_revenue_daily.sql
│       ├── mart_top_products.sql
│       └── mart_customer_ltv.sql
├── src/
│   ├── __init__.py
│   ├── data_generator.py          # Génération Faker
│   ├── validators.py              # Validation schéma
│   └── utils.py                   # Helpers
├── tests/
│   ├── __init__.py
│   ├── test_validators.py
│   ├── test_data_generator.py
│   └── test_sql_checks.py
├── docker-compose.yml
├── Dockerfile
├── .env.example
├── .gitignore
├── .pre-commit-config.yaml
├── pyproject.toml
├── requirements.txt
└── README.md
```

---

## ✅ Data Quality

### Checks RAW (post-ingestion)

| Check | Table | Règle |
|-------|-------|-------|
| `count_check` | `raw.*` | COUNT(*) > 0 |
| `null_pk_check` | `raw_orders` | order_id NOT NULL |
| `date_valid_check` | `raw_orders` | order_date dans plage valide |

### Checks MART (post-transformation)

| Check | Table | Règle |
|-------|-------|-------|
| `revenue_positive` | `mart.revenue_daily` | total_revenue >= 0 |
| `no_duplicates` | `mart.revenue_daily` | COUNT(DISTINCT date) = COUNT(*) |
| `freshness` | `mart.revenue_daily` | MAX(date) >= CURRENT_DATE - 1 |

---

## 🧪 Tests

```bash
# Lancer tous les tests
pytest tests/ -v

# Avec couverture
pytest tests/ --cov=src --cov-report=html

# Tests unitaires seulement
pytest tests/test_validators.py tests/test_data_generator.py -v

# Tests SQL
pytest tests/test_sql_checks.py -v
```

### Structure des tests

- `test_validators.py` : validation schéma CSV
- `test_data_generator.py` : génération données Faker
- `test_sql_checks.py` : requêtes de contrôle SQL

---

## 🎯 Compétences démontrées

| Domaine | Compétences |
|---------|-------------|
| **Orchestration** | Apache Airflow, TaskFlow API, DAG design, dependencies |
| **Data Engineering** | ELT pipelines, batch processing, idempotence |
| **SQL** | Postgres, window functions, CTEs, analytics |
| **Data Quality** | Schema validation, assertions, monitoring |
| **DevOps** | Docker, Docker Compose, CI/CD, GitHub Actions |
| **Python** | Type hints, logging, testing, Faker |
| **Best Practices** | Retry/backoff, timeouts, SLA, documentation |

---

## 🗺️ Roadmap

- [ ] **S3 Integration** : Remplacer CSV local par MinIO/S3
- [ ] **dbt** : Migrer transformations SQL vers dbt
- [ ] **Great Expectations** : Data quality framework
- [ ] **Data Lineage** : OpenLineage + Marquez
- [ ] **Alerting** : Slack/Email notifications
- [ ] **Metrics** : Prometheus + Grafana dashboards
- [ ] **Partitioning** : Tables partitionnées par date
- [ ] **CDC** : Debezium pour ingestion temps réel

---

## 📝 Exemple section CV

> **Airflow E-Commerce Data Pipeline** | Python, SQL, Airflow, Docker
>
> - Conçu et implémenté un pipeline ELT orchestré par Airflow traitant **10k+ transactions/jour** avec 3 couches de données (raw/staging/mart)
> - Automatisé les contrôles qualité (schema validation, null checks, freshness) réduisant les erreurs de données de **95%**
> - Conteneurisé l'infrastructure (Docker Compose) avec CI/CD GitHub Actions, permettant un déploiement reproductible en **< 5 minutes**

---

## 📄 License

MIT License - voir [LICENSE](LICENSE)

---

## 👤 Auteur

**Bentz Louis**  
Data Engineer | [LinkedIn](https://www.linkedin.com/in/louis-bentz-40a9851b3/)) | [GitHub](https://github.com/louisbentz2002-crypto)

