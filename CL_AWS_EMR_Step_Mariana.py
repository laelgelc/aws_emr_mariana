import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, col, array_contains

# Set the S3 bucket and folder paths
parser = argparse.ArgumentParser()
parser.add_argument(
    '--data_source', help='The S3 URI of the data source.')
parser.add_argument(
    '--output_path', help="The S3 URI of the output path.")
args = parser.parse_args()

# Create a SparkSession
spark = SparkSession.builder.appName('The Twitter Grab 2019 Corpus').getOrCreate()

# Read the JSONL files into a DataFrame
#tweets_spark_df = spark.read.json(args.data_source) # RevA parameters
tweets_spark_df = spark.read.option('recursiveFileLookup', 'true').json(args.data_source) # RevB parameters

# Define the list of hashtags for DataFrame filtering
hashtags = [
    'acolhimento', 
    'Acolhimento', 
    'ACOLHIMENTO', 
    'aporofobia', 
    'Aporofobia', 
    'APOROFOBIA', 
    'brasilvaivirarvenezuela', 
    'Brasilvaivirarvenezuela', 
    'BrasilVaiVirarVenezuela', 
    'BRASILVAIVIRARVENEZUELA', 
    'crisehumanitária', 
    'Crisehumanitária', 
    'CriseHumanitária', 
    'CRISEHUMANITÁRIA', 
    'crisevenezuelana', 
    'Crisevenezuelana', 
    'CriseVenezuelana', 
    'CRISEVENEZUELANA', 
    'Discriminação', 
    'discriminação', 
    'DISCRIMINAÇÃO', 
    'estereótipo', 
    'Estereótipo', 
    'ESTEREÓTIPO', 
    'fronteira', 
    'Fronteira', 
    'FRONTEIRA', 
    'migrantes', 
    'Migrantes', 
    'MIGRANTES', 
    'preconceito', 
    'Preconceito', 
    'PRECONCEITO', 
    'refugiados', 
    'Refugiados', 
    'REFUGIADOS', 
    'roraizuela', 
    'Roraizuela', 
    'RORAIZUELA', 
    'venebrasil', 
    'Venebrasil', 
    'VeneBrasil', 
    'VENEBRASIL', 
    'venezuelanosnobrasil', 
    'Venezuelanosnobrasil', 
    'VenezuelanosNoBrasil', 
    'VENEZUELANOSNOBRASIL', 
    'venezuraima', 
    'Venezuraima', 
    'VENEZURAIMA', 
    'violência', 
    'Violência', 
    'VIOLÊNCIA', 
    'xenofobia', 
    'Xenofobia', 
    'XENOFOBIA'
]

expressions = [
    'ameaça', 
    'aporofobia', 
    'carga', 
    'conflito', 
    'crise', 
    'delinquência', 
    'desconfiança', 
    'desemprego', 
    'desigualdade', 
    'desordem', 
    'direitos', 
    'discriminação', 
    'estigma', 
    'estrangeiro', 
    'exclusão', 
    'fronteira',  
    'hostilidade', 
    'humanitário', 
    'identidade', 
    'inferior', 
    'intolerância', 
    'invasão', 
    'marginal', 
    'perigo', 
    'preconceito', 
    'problema', 
    'racismo', 
    'refugiada', 
    'refugiado', 
    'refugiados', 
    'rejeição', 
    'roraizuela', 
    'venebrasil', 
    'venezuela', 
    'venezuelana', 
    'venezuelanas', 
    'venezuelano', 
    'venezuelanos', 
    'venezuraima', 
    'violência', 
    'xenofobia'
]

# Create a filtered DataFrame
filtered_tweets_spark_df = tweets_spark_df.filter(
    array_contains('entities.hashtags.text', hashtags[0]) |\
    array_contains('entities.hashtags.text', hashtags[1]) |\
    array_contains('entities.hashtags.text', hashtags[2]) |\
    array_contains('entities.hashtags.text', hashtags[3]) |\
    array_contains('entities.hashtags.text', hashtags[4]) |\
    array_contains('entities.hashtags.text', hashtags[5]) |\
    array_contains('entities.hashtags.text', hashtags[6]) |\
    array_contains('entities.hashtags.text', hashtags[7]) |\
    array_contains('entities.hashtags.text', hashtags[8]) |\
    array_contains('entities.hashtags.text', hashtags[9]) |\
    array_contains('entities.hashtags.text', hashtags[10]) |\
    array_contains('entities.hashtags.text', hashtags[11]) |\
    array_contains('entities.hashtags.text', hashtags[12]) |\
    array_contains('entities.hashtags.text', hashtags[13]) |\
    array_contains('entities.hashtags.text', hashtags[14]) |\
    array_contains('entities.hashtags.text', hashtags[15]) |\
    array_contains('entities.hashtags.text', hashtags[16]) |\
    array_contains('entities.hashtags.text', hashtags[17]) |\
    array_contains('entities.hashtags.text', hashtags[18]) |\
    array_contains('entities.hashtags.text', hashtags[19]) |\
    array_contains('entities.hashtags.text', hashtags[20]) |\
    array_contains('entities.hashtags.text', hashtags[21]) |\
    array_contains('entities.hashtags.text', hashtags[22]) |\
    array_contains('entities.hashtags.text', hashtags[23]) |\
    array_contains('entities.hashtags.text', hashtags[24]) |\
    array_contains('entities.hashtags.text', hashtags[25]) |\
    array_contains('entities.hashtags.text', hashtags[26]) |\
    array_contains('entities.hashtags.text', hashtags[27]) |\
    array_contains('entities.hashtags.text', hashtags[28]) |\
    array_contains('entities.hashtags.text', hashtags[29]) |\
    array_contains('entities.hashtags.text', hashtags[30]) |\
    array_contains('entities.hashtags.text', hashtags[31]) |\
    array_contains('entities.hashtags.text', hashtags[32]) |\
    array_contains('entities.hashtags.text', hashtags[33]) |\
    array_contains('entities.hashtags.text', hashtags[34]) |\
    array_contains('entities.hashtags.text', hashtags[35]) |\
    array_contains('entities.hashtags.text', hashtags[36]) |\
    array_contains('entities.hashtags.text', hashtags[37]) |\
    array_contains('entities.hashtags.text', hashtags[38]) |\
    array_contains('entities.hashtags.text', hashtags[39]) |\
    array_contains('entities.hashtags.text', hashtags[40]) |\
    array_contains('entities.hashtags.text', hashtags[41]) |\
    array_contains('entities.hashtags.text', hashtags[42]) |\
    array_contains('entities.hashtags.text', hashtags[43]) |\
    array_contains('entities.hashtags.text', hashtags[44]) |\
    array_contains('entities.hashtags.text', hashtags[45]) |\
    array_contains('entities.hashtags.text', hashtags[46]) |\
    array_contains('entities.hashtags.text', hashtags[47]) |\
    array_contains('entities.hashtags.text', hashtags[48]) |\
    array_contains('entities.hashtags.text', hashtags[49]) |\
    array_contains('entities.hashtags.text', hashtags[50]) |\
    array_contains('entities.hashtags.text', hashtags[51]) |\
    array_contains('entities.hashtags.text', hashtags[52]) |\
    array_contains('entities.hashtags.text', hashtags[53]) |\
    array_contains('entities.hashtags.text', hashtags[54]) |\
    array_contains('entities.hashtags.text', hashtags[55]) |\
    lower(col('text')).contains(expressions[0]) |\
    lower(col('text')).contains(expressions[1]) |\
    lower(col('text')).contains(expressions[2]) |\
    lower(col('text')).contains(expressions[3]) |\
    lower(col('text')).contains(expressions[4]) |\
    lower(col('text')).contains(expressions[5]) |\
    lower(col('text')).contains(expressions[6]) |\
    lower(col('text')).contains(expressions[7]) |\
    lower(col('text')).contains(expressions[8]) |\
    lower(col('text')).contains(expressions[9]) |\
    lower(col('text')).contains(expressions[10]) |\
    lower(col('text')).contains(expressions[11]) |\
    lower(col('text')).contains(expressions[12]) |\
    lower(col('text')).contains(expressions[13]) |\
    lower(col('text')).contains(expressions[14]) |\
    lower(col('text')).contains(expressions[15]) |\
    lower(col('text')).contains(expressions[16]) |\
    lower(col('text')).contains(expressions[17]) |\
    lower(col('text')).contains(expressions[18]) |\
    lower(col('text')).contains(expressions[19]) |\
    lower(col('text')).contains(expressions[20]) |\
    lower(col('text')).contains(expressions[21]) |\
    lower(col('text')).contains(expressions[22]) |\
    lower(col('text')).contains(expressions[23]) |\
    lower(col('text')).contains(expressions[24]) |\
    lower(col('text')).contains(expressions[25]) |\
    lower(col('text')).contains(expressions[26]) |\
    lower(col('text')).contains(expressions[27]) |\
    lower(col('text')).contains(expressions[28]) |\
    lower(col('text')).contains(expressions[29]) |\
    lower(col('text')).contains(expressions[30]) |\
    lower(col('text')).contains(expressions[31]) |\
    lower(col('text')).contains(expressions[32]) |\
    lower(col('text')).contains(expressions[33]) |\
    lower(col('text')).contains(expressions[34]) |\
    lower(col('text')).contains(expressions[35]) |\
    lower(col('text')).contains(expressions[36]) |\
    lower(col('text')).contains(expressions[37]) |\
    lower(col('text')).contains(expressions[38]) |\
    lower(col('text')).contains(expressions[39]) |\
    lower(col('text')).contains(expressions[40])
)

# Export the DataFrame to JSONL format
filtered_tweets_spark_df.write.mode('overwrite').json(args.output_path)
