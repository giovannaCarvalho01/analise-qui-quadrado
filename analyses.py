import psycopg2
import numpy as np
from scipy.stats import chi2_contingency
import dotenv
import os

# Para pegar as variveis do arquivo .env
dotenv.load_dotenv()
    
# Conectar ao banco de dados PostgreSQL
conn = psycopg2.connect(
    dbname= os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD'),
    host=os.getenv('DB_HOST'),  
    port=os.getenv('DB_PORT')  # porta padrão do PostgreSQL
)

cur = conn.cursor()

# Executar a consulta SQL para obter os dados necessários
query = """
WITH max_nota AS (
    SELECT MAX(nota_geral) AS max_nota_geral
    FROM fact_notas
    WHERE tipo_presenca <> '222'
),
classified_notas AS (
    SELECT 
        ft.nota_geral,
        CASE 
            WHEN ft.nota_geral < (mn.max_nota_geral * 0.6) THEN 'BAIXO'
            ELSE 'ALTO'
        END AS classificacao,
        sx.sexo
    FROM fact_notas AS ft
    CROSS JOIN max_nota AS mn
    INNER JOIN dim_sexo AS sx
        ON sx.id = ft.id
    WHERE ft.tipo_presenca <> '222'
)
SELECT
    classificacao,
    sexo,
    COUNT(*) as count
FROM classified_notas
GROUP BY classificacao, sexo
ORDER BY classificacao, sexo;
"""

# Executar a consulta e obter os resultados
cur.execute(query)
resultados = cur.fetchall()

# Estruturar os dados em uma tabela de contingência
contingency_table = {
    'BAIXO': {'F': 0, 'M': 0},
    'ALTO': {'F': 0, 'M': 0}
}

# Preencher a tabela de contingência com os resultados da consulta
for row in resultados:
    classificacao, sexo, count = row
    contingency_table[classificacao][sexo] = count

# Converter a tabela de contingência para um array numpy
tabela_contingencia = np.array([
    [contingency_table['BAIXO']['F'], contingency_table['BAIXO']['M']],
    [contingency_table['ALTO']['F'], contingency_table['ALTO']['M']]
])

# Verificar a tabela de contingência
print("Tabela de contingência:")
print(tabela_contingencia)

# Fechar a conexão com o banco de dados
cur.close()
conn.close()

# Calcular o teste do qui-quadrado sem a correção de Yates
chi2, p, dof, expected = chi2_contingency(tabela_contingencia, correction=False)

# Exibir os resultados ajustados
print(f"Valor do qui-quadrado (sem correção de Yates): {chi2}")
print(f"Valor p: {p}")
print(f"Graus de liberdade: {dof}")
print("Frequências esperadas (sem correção):")
print(expected)