# Consultas — NYC TLC

Queries de análise dos dados de táxi de Nova York disponíveis para execução no Amazon Athena ou via notebook.

---

## Como executar no Athena

```
AWS Console → Athena → Query editor
```

Antes de executar, configure o output das queries:

```
Settings → Manage → S3 location → s3://{bucket}/athena-results/
```

---

## Query 1 — Média de `total_amount` por mês — Yellow Taxi

Calcula a média do valor total das corridas por mês do Yellow Taxi de Janeiro a Maio de 2023.

```sql
SELECT mes, avg_total_amount
FROM ifood_case_gold.table_avg_total_amount_gold
ORDER BY mes;
```

**Resultado:**

| mes | avg_total_amount |
|---|---|
| 1 | 27.46 |
| 2 | 27.37 |
| 3 | 28.29 |
| 4 | 28.78 |
| 5 | 29.45 |

---

## Query 2 — Média de `passenger_count` por hora — Todos os táxis (Maio 2023)

Calcula a média de passageiros por hora do dia considerando Yellow e Green Taxi em maio de 2023.

```sql
SELECT hora, avg_passenger_count
FROM ifood_case_gold.table_avg_passengers_gold
ORDER BY hora;
```

**Resultado (amostra):**

| hora | avg_passenger_count |
|---|---|
| 0 | 1.43 |
| 1 | 1.44 |
| 5 | 1.28 |
| 6 | 1.26 |
| 14 | 1.39 |
| 23 | 1.42 |

---

## Notebook

O output completo das duas queries com visualizações está disponível em [queries.ipynb](queries.ipynb).
