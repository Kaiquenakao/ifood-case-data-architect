# Modelagem de Dados — Glue Data Catalog

Documentação completa dos schemas, particionamentos e decisões de modelagem das tabelas do Data Lake iFood Case, organizadas por camada da arquitetura Medallion.

Cada tabela está implementada em um arquivo `.tf` dedicado neste diretório, com comentários de modelagem alinhados a esta documentação.

---

## Bronze — `ifood_case_bronze`

Dados brutos ingeridos diretamente da CDN do NYC TLC sem transformações. Preserva o schema original da fonte para garantir rastreabilidade e reprocessamento.

![Glue Data Catalog](../../../doc/glue_data_catalog.png)
*AWS Console → Glue → Data Catalog → Databases → `ifood_case_bronze` / `ifood_case_silver` / `ifood_case_gold` → Tables*

### `table_yellow_taxi_bronze`

**Arquivo:** `catalog/bronze/crawler_yellow_taxi_bronze.tf`
**Path S3:** `s3://{bucket}/bronze/table_yellow_taxi_bronze/`
**Registro de partições:** automático via Glue Crawler após ETL Bronze

| Coluna | Tipo | Descrição |
|---|---|---|
| `vendorid` | bigint | ID do fornecedor do táxi |
| `tpep_pickup_datetime` | timestamp | Data e hora de início da corrida |
| `tpep_dropoff_datetime` | timestamp | Data e hora de fim da corrida |
| `passenger_count` | double | Número de passageiros |
| `trip_distance` | double | Distância percorrida em milhas |
| `ratecodeid` | double | Código da tarifa aplicada |
| `store_and_fwd_flag` | string | Flag de armazenamento offline |
| `pulocationid` | bigint | ID da zona de embarque |
| `dolocationid` | bigint | ID da zona de desembarque |
| `payment_type` | bigint | Tipo de pagamento |
| `fare_amount` | double | Valor da tarifa base |
| `extra` | double | Extras e sobretaxas |
| `mta_tax` | double | Taxa MTA |
| `tip_amount` | double | Gorjeta |
| `tolls_amount` | double | Pedágios |
| `improvement_surcharge` | double | Taxa de melhoria |
| `total_amount` | double | Valor total da corrida em USD |
| `congestion_surcharge` | double | Taxa de congestionamento |
| `airport_fee` | double | Taxa de aeroporto (exclusiva Yellow) |
| `partition_year` | int | Partição — ano da corrida |
| `partition_month` | int | Partição — mês da corrida |

---

### `table_green_taxi_bronze`

**Arquivo:** `catalog/bronze/crawler_green_taxi_bronze.tf`
**Path S3:** `s3://{bucket}/bronze/table_green_taxi_bronze/`
**Registro de partições:** automático via Glue Crawler após ETL Bronze

| Coluna | Tipo | Descrição |
|---|---|---|
| `vendorid` | bigint | ID do fornecedor do táxi |
| `lpep_pickup_datetime` | timestamp | Data e hora de início da corrida |
| `lpep_dropoff_datetime` | timestamp | Data e hora de fim da corrida |
| `store_and_fwd_flag` | string | Flag de armazenamento offline |
| `ratecodeid` | double | Código da tarifa aplicada |
| `pulocationid` | bigint | ID da zona de embarque |
| `dolocationid` | bigint | ID da zona de desembarque |
| `passenger_count` | double | Número de passageiros |
| `trip_distance` | double | Distância percorrida em milhas |
| `fare_amount` | double | Valor da tarifa base |
| `extra` | double | Extras e sobretaxas |
| `mta_tax` | double | Taxa MTA |
| `tip_amount` | double | Gorjeta |
| `tolls_amount` | double | Pedágios |
| `ehail_fee` | double | Taxa de e-hail (exclusiva Green) |
| `improvement_surcharge` | double | Taxa de melhoria |
| `total_amount` | double | Valor total da corrida em USD |
| `payment_type` | double | Tipo de pagamento |
| `trip_type` | double | Tipo de corrida |
| `congestion_surcharge` | double | Taxa de congestionamento |
| `partition_year` | int | Partição — ano da corrida |
| `partition_month` | int | Partição — mês da corrida |

---

## Silver — `ifood_case_silver`

Dados transformados e padronizados. Schema unificado entre Yellow e Green Taxi — ambas as tabelas têm as mesmas colunas, permitindo UNION direto na camada Gold.

**Decisões de modelagem:**
- `vendorid` renomeado para `vendor_id` — padronização snake_case
- `tpep_pickup_datetime` / `lpep_pickup_datetime` unificados em `pickup_datetime`
- `tpep_dropoff_datetime` / `lpep_dropoff_datetime` unificados em `dropoff_datetime`
- Coluna `taxi_type` adicionada com valor literal `yellow` ou `green` para identificação após UNION
- Nulos removidos nas colunas obrigatórias: `vendor_id`, `passenger_count`, `total_amount`, `pickup_datetime`, `dropoff_datetime`
- Partições registradas automaticamente via `MSCK REPAIR TABLE` no ETL Silver

### `table_yellow_taxi_silver`

**Arquivo:** `catalog/silver/table_yellow_taxi_silver.tf`
**Path S3:** `s3://{bucket}/silver/table_yellow_taxi_silver/`
**Origem:** `ifood_case_bronze.table_yellow_taxi_bronze`

### `table_green_taxi_silver`

**Arquivo:** `catalog/silver/table_green_taxi_silver.tf`
**Path S3:** `s3://{bucket}/silver/table_green_taxi_silver/`
**Origem:** `ifood_case_bronze.table_green_taxi_bronze`

**Schema (Yellow e Green — idêntico):**

| Coluna | Tipo | Descrição |
|---|---|---|
| `vendor_id` | int | ID do fornecedor do táxi |
| `passenger_count` | int | Número de passageiros — nulos removidos |
| `total_amount` | double | Valor total da corrida em USD — nulos removidos |
| `pickup_datetime` | timestamp | Data e hora de início da corrida |
| `dropoff_datetime` | timestamp | Data e hora de fim da corrida |
| `taxi_type` | string | Tipo do táxi — `yellow` ou `green` |
| `partition_year` | int | Partição — ano da corrida |
| `partition_month` | int | Partição — mês da corrida |

---

## Gold — `ifood_case_gold`

Dados consolidados e pré-agregados para consumo analítico de alta performance no Athena.

### `table_all_taxi_gold`

**Arquivo:** `catalog/gold/table_all_taxi_gold.tf`
**Path S3:** `s3://{bucket}/gold/table_all_taxi_gold/`
**Origem:** UNION de `table_yellow_taxi_silver` + `table_green_taxi_silver`
**Partições:** registradas automaticamente via `MSCK REPAIR TABLE` no ETL Gold

Tabela base da camada Gold — centraliza os filtros de qualidade para garantir consistência entre todas as análises derivadas.

**Filtros de qualidade aplicados:**
- `total_amount > 0`
- `passenger_count` entre 1 e 6
- Corridas de Janeiro a Maio de 2023

| Coluna | Tipo | Descrição |
|---|---|---|
| `VendorID` | int | ID do fornecedor do táxi |
| `passenger_count` | int | Número de passageiros — entre 1 e 6 |
| `total_amount` | double | Valor total em USD — maior que zero |
| `tpep_pickup_datetime` | timestamp | Data e hora de início da corrida |
| `tpep_dropoff_datetime` | timestamp | Data e hora de fim da corrida |
| `taxi_type` | string | Tipo do táxi — `yellow` ou `green` |
| `partition_year` | int | Partição — ano da corrida |
| `partition_month` | int | Partição — mês da corrida |

---