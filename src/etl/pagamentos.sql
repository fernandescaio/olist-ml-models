-- Databricks notebook source
SELECT *
        
FROM silver.olist.pedido

WHERE dtPedido < '2018-01-01'
AND dtPedido >= add_months('2018-01-01', -6)


-- COMMAND ----------


WITH tb_join AS (

SELECT t2.*,
        t3.idVendedor
        
FROM silver.olist.pedido AS t1

LEFT JOIN silver.olist.pagamento_pedido AS t2
ON t1.idPedido = t2.idPedido

LEFT JOIN silver.olist.item_pedido AS t3
ON t1.idPedido = t3.idPedido

WHERE t1.dtPedido < '2018-01-01'
AND t1.dtPedido >= add_months('2018-01-01', -6)
and t3.idVendedor IS NOT NULL

),

tb_group AS (

SELECT idVendedor,
        descTipoPagamento,
        count(distinct idPedido) AS qtdePedido,
        sum(vlPagamento) AS vlPedidoMeioPagamento

FROM tb_join

GROUP BY idVendedor, descTipoPagamento
ORDER BY idVendedor, descTipoPagamento

)

SELECT  idVendedor,
        sum( CASE WHEN descTipoPagamento = 'boleto' THEN qtdePedidoMeioPagamento ELSE 0 END) AS qtde_boleto,
        sum( CASE WHEN descTipoPagamento = 'credit_card' THEN qtdePedidoMeioPagamento ELSE 0 END) AS qtde_credit_card,
        sum( CASE WHEN descTipoPagamento = 'voucher' THEN qtdePedidoMeioPagamento ELSE 0 END) AS qtde_voucher,
        sum( CASE WHEN descTipoPagamento = 'debit_card' THEN qtdePedidoMeioPagamento ELSE 0 END) AS qtde_debit_card,

        sum( CASE WHEN descTipoPagamento = 'boleto' THEN vlPedidoMeioPagamento ELSE 0 END) AS valor_boleto,
        sum( CASE WHEN descTipoPagamento = 'credit_card' THEN vlPedidoMeioPagamento ELSE 0 END) AS valor_credit_card,
        sum( CASE WHEN descTipoPagamento = 'voucher' THEN vlPedidoMeioPagamento ELSE 0 END) AS valor_voucher,
        sum( CASE WHEN descTipoPagamento = 'debit_card' THEN vlPedidoMeioPagamento ELSE 0 END) AS valor_debit_car

FROM tb_group
