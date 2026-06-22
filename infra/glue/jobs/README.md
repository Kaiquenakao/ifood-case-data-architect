# Glue Jobs — Referência Operacional

Documentação dos jobs AWS Glue que compõem o pipeline NYC TLC, com recursos computacionais e tempos de execução de referência.

Todos os jobs compartilham a mesma configuração de recursos — apenas o script e o tempo de execução variam por camada.

---

## Configuração de Recursos

| Parâmetro | Valor |
|---|---|
| Glue Version | 4.0 |
| Worker Type | G.1X |
| Number of Workers | 10 |
| Max Capacity | 10 DPUs |
| Timeout | 2880 minutos |
| Python Version | 3 |

---

## Bronze — `ifood_case_bronze`

Jobs responsáveis pela ingestão dos dados brutos do NYC TLC via download HTTP, com padronização de colunas e tipagem explícita via schema.

| Job | Script | Tempo médio |
|---|---|---|
| `ifood-case-prod-etl-yellow-taxi-bronze` | `src/bronze/etl_yellow_taxi_bronze.py` | 1m 07s |
| `ifood-case-prod-etl-green-taxi-bronze` | `src/bronze/etl_green_taxi_bronze.py` | 53s |

---

## Silver — `ifood_case_silver`

Jobs responsáveis pela transformação e padronização dos dados Bronze. Leem via Glue Catalog, aplicam schema unificado e removem nulos nas colunas obrigatórias.

| Job | Script | Tempo médio |
|---|---|---|
| `ifood-case-prod-etl-yellow-taxi-silver` | `src/silver/etl_yellow_taxi_silver.py` | 3m 33s |
| `ifood-case-prod-etl-green-taxi-silver` | `src/silver/etl_green_taxi_silver.py` | 1m 16s |

> O Yellow Taxi demora mais por ter volume significativamente maior de corridas que o Green.

---

## Gold — `ifood_case_gold`

Jobs responsáveis pela consolidação e agregação dos dados Silver para consumo analítico no Athena.

| Job | Script | Tempo médio |
|---|---|---|
| `ifood-case-prod-etl-all-taxi-gold` | `src/gold/etl_all_taxi_gold.py` | 2m 45s |
| `ifood-case-prod-etl-avg-total-amount-gold` | `src/gold/etl_avg_total_amount_gold.py` | 55s |
| `ifood-case-prod-etl-avg-passengers-gold` | `src/gold/etl_avg_passengers_gold.py` | 1m 02s |

> `etl_all_taxi_gold` é o mais custoso por realizar UNION ALL de Yellow e Green com filtros de qualidade sobre todo o dataset.

---

## Tempo Total do Pipeline

| Etapa | Execução | Tempo |
|---|---|---|
| Bronze | Paralelo | ~1m 07s |
| Crawler Bronze | Paralelo | ~1m |
| Silver | Paralelo | ~3m 33s |
| Gold All Taxi | Sequencial | ~2m 45s |
| Gold Agregações | Paralelo | ~1m 02s |
| **Total** | | **~10-12 min** |

> Os tempos paralelos são limitados pelo job mais lento da etapa.
