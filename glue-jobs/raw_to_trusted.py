import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

# Configuração de logging 
logger = logging.getLogger("raw_to_trusted")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger.info("Iniciando job raw_to_trusted")

# CLIENTES 
clientes_raw = glueContext.create_dynamic_frame.from_catalog(
    database="lgpd_vault_raw", table_name="clientes"
).toDF()

total_raw = clientes_raw.count()
logger.info(f"Clientes lidos da raw: {total_raw}")

clientes_trusted = (
    clientes_raw
    .dropDuplicates(["cliente_id"])
    .dropna(subset=["cpf", "cliente_id"])
)

total_trusted = clientes_trusted.count()
removidos = total_raw - total_trusted
logger.info(f"Clientes apos limpeza: {total_trusted} (removidos: {removidos})")

if removidos > 0:
    logger.warning(f"{removidos} registros de clientes removidos por duplicidade ou campos nulos")

clientes_trusted.write.mode("overwrite").parquet(
    "s3://lgpd-vault-gentleman0xc-trusted/clientes/"
)
logger.info("Clientes gravados na camada trusted")

# TRANSACOES 
transacoes_raw = glueContext.create_dynamic_frame.from_catalog(
    database="lgpd_vault_raw", table_name="transacoes"
).toDF()

total_raw_t = transacoes_raw.count()
logger.info(f"Transacoes lidas da raw: {total_raw_t}")

transacoes_trusted = (
    transacoes_raw
    .dropDuplicates(["transacao_id"])
    .dropna(subset=["cliente_id", "valor"])
)

total_trusted_t = transacoes_trusted.count()
removidos_t = total_raw_t - total_trusted_t
logger.info(f"Transacoes apos limpeza: {total_trusted_t} (removidos: {removidos_t})")

transacoes_trusted.write.mode("overwrite").parquet(
    "s3://lgpd-vault-gentleman0xc-trusted/transacoes/"
)
logger.info("Transacoes gravadas na camada trusted")

logger.info("Job raw_to_trusted finalizado com sucesso")
job.commit()