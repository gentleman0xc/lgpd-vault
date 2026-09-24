import sys
import logging
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, concat_ws, year, month

logger = logging.getLogger("raw_to_trusted")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)


# ---------------------------------------------------------------------------
# Funcoes puras de transformacao — testaveis com pytest + SparkSession local,
# sem depender do ambiente Glue (I/O fica separado, mais abaixo).
# ---------------------------------------------------------------------------

def separar_validos_e_rejeitados(df: DataFrame, colunas_obrigatorias: list, chave: str) -> tuple[DataFrame, DataFrame]:
    """
    Separa um DataFrame em (validos, rejeitados) em vez de descartar
    registros silenciosamente. Cada registro aparece em EXATAMENTE um
    dos dois DataFrames de saida (nunca nos dois).

    motivo_rejeicao concatena todos os problemas encontrados na linha,
    ex: "chave_duplicada;campo_nulo:cpf" — um registro pode ter mais de
    um problema simultaneamente.

    Nota de design: uma chave duplicada reprova TODAS as suas ocorrencias
    (nao existe "sobrevivente automatico"). Resolver duplicidade e uma
    decisao que exige intervencao, nao deve ser feita silenciosamente
    pelo pipeline.
    """
    contagem_por_chave = (
        df.groupBy(chave).count().withColumnRenamed("count", "_qtd_ocorrencias")
    )
    df_marcado = df.join(contagem_por_chave, on=chave, how="left")
    df_marcado = df_marcado.withColumn("_dup", col("_qtd_ocorrencias") > 1)

    colunas_flag_nula = []
    for coluna in colunas_obrigatorias:
        flag = f"_nulo_{coluna}"
        df_marcado = df_marcado.withColumn(flag, col(coluna).isNull())
        colunas_flag_nula.append(flag)

    partes_motivo = [when(col("_dup"), lit("chave_duplicada"))]
    for coluna, flag in zip(colunas_obrigatorias, colunas_flag_nula):
        partes_motivo.append(when(col(flag), lit(f"campo_nulo:{coluna}")))

    df_marcado = df_marcado.withColumn("motivo_rejeicao", concat_ws(";", *partes_motivo))

    tem_problema = col("_dup")
    for flag in colunas_flag_nula:
        tem_problema = tem_problema | col(flag)

    colunas_auxiliares = ["_qtd_ocorrencias", "_dup"] + colunas_flag_nula

    rejeitados = df_marcado.filter(tem_problema).drop(*colunas_auxiliares)
    validos = df_marcado.filter(~tem_problema).drop(*colunas_auxiliares, "motivo_rejeicao")

    return validos, rejeitados


def adicionar_particao_temporal(df: DataFrame, coluna_data: str) -> DataFrame:
    """Deriva colunas ano/mes de uma coluna de data/timestamp, para particionamento."""
    return df.withColumn("ano", year(col(coluna_data))).withColumn("mes", month(col(coluna_data)))


# ---------------------------------------------------------------------------
# Execucao (especifico do ambiente Glue)
# ---------------------------------------------------------------------------

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger.info("Iniciando job raw_to_trusted")

# --- CLIENTES ---
clientes_raw = glueContext.create_dynamic_frame.from_catalog(
    database="lgpd_vault_raw", table_name="clientes"
).toDF()

total_raw = clientes_raw.count()
logger.info(f"Clientes lidos da raw: {total_raw}")

clientes_validos, clientes_rejeitados = separar_validos_e_rejeitados(
    clientes_raw, colunas_obrigatorias=["cpf", "cliente_id"], chave="cliente_id"
)

total_validos = clientes_validos.count()
total_rejeitados = clientes_rejeitados.count()
logger.info(f"Clientes validos: {total_validos} | rejeitados: {total_rejeitados}")

if total_rejeitados > 0:
    logger.warning(f"{total_rejeitados} registros de clientes movidos para quarentena")
    clientes_rejeitados.write.mode("append").parquet(
        "s3://lgpd-vault-gentleman0xc-raw/quarantine/clientes/"
    )

clientes_validos.write.mode("overwrite").parquet(
    "s3://lgpd-vault-gentleman0xc-trusted/clientes/"
)
logger.info("Clientes gravados na camada trusted")

# --- TRANSACOES ---
transacoes_raw = glueContext.create_dynamic_frame.from_catalog(
    database="lgpd_vault_raw", table_name="transacoes"
).toDF()

total_raw_t = transacoes_raw.count()
logger.info(f"Transacoes lidas da raw: {total_raw_t}")

transacoes_validas, transacoes_rejeitadas = separar_validos_e_rejeitados(
    transacoes_raw, colunas_obrigatorias=["cliente_id", "valor"], chave="transacao_id"
)

total_validas_t = transacoes_validas.count()
total_rejeitadas_t = transacoes_rejeitadas.count()
logger.info(f"Transacoes validas: {total_validas_t} | rejeitadas: {total_rejeitadas_t}")

if total_rejeitadas_t > 0:
    logger.warning(f"{total_rejeitadas_t} registros de transacoes movidos para quarentena")
    transacoes_rejeitadas.write.mode("append").parquet(
        "s3://lgpd-vault-gentleman0xc-raw/quarantine/transacoes/"
    )

transacoes_validas = adicionar_particao_temporal(transacoes_validas, "data")

transacoes_validas.write.mode("overwrite").partitionBy("ano", "mes").parquet(
    "s3://lgpd-vault-gentleman0xc-trusted/transacoes/"
)
logger.info("Transacoes gravadas na camada trusted, particionadas por ano/mes")

logger.info("Job raw_to_trusted finalizado com sucesso")
job.commit()