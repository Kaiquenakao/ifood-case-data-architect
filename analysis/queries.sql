-- =============================================================
-- Case Técnico Data Architect — iFood
-- Queries de Análise — Camada Gold
-- Database: ifood_case_gold
-- =============================================================

-- -------------------------------------------------------------
-- Query 1
-- Qual a média de valor total (total_amount) recebido em um mês
-- considerando todos os yellow táxis da frota?
-- -------------------------------------------------------------
SELECT
    mes,
    avg_total_amount
FROM ifood_case_gold.table_avg_total_amount_gold
ORDER BY mes;


-- -------------------------------------------------------------
-- Query 2
-- Qual a média de passageiros (passenger_count) por cada hora
-- do dia que pegaram táxi no mês de maio considerando todos
-- os táxis da frota?
-- -------------------------------------------------------------
SELECT
    hora,
    avg_passenger_count
FROM ifood_case_gold.table_avg_passengers_gold
ORDER BY hora;
