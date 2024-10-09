## **Análise de Logs Web com Apache Spark**

# Descrição 
Para responder ao desafio "Assignment - Data Engineer" que fora proposto, foi realizado o download do arquivo de log, através do site a seguir: https://www.kaggle.com/datasets/eliasdabbas/web-server-access-logs/data. Importante ressaltar que o arquivo obtido foi reduzido à 1000 (mil) registros, tendo em vista, que o objetivo é, apenas, obter uma amostra para responder ao desafio.

O desenvolvimento foi baseado no desafio Assignment - Data Engineer, objetivando processar um arquivo de log no padrão Web Access Server Log para extrair informações relevantes para estruturar um dataframe que, no final, será salvo como uma tabela Delta. Para realizar as transformações, foi utilizado spark com a linguagem Scala.

## **Instruções 
Para que o projeto seja executado com sucesso, será necessário criar uma estrutura de pastas no DBFS do databricks community. Para isso,
basta seguir o passo a passo abaixo:

1 - Acessar o DBFS do databrick e criar uma pasta chamada camadas

2 - Dentro da pasta camadas criar mais três pastas, listadas consecutivamente abaixo: 
  - bronze     (dbfs:/camadas/bronze)
  - silver     (dbfs:/camadas/silver)
  - gold       (dbfs:/camadas/gold)

3 - baixar, do gitHub ou e-mail, o arquivo B_arqv.log e salvá-lo na pasta bronze que fora criada anteriormente dentro da pasta camadas, ou seja: dbfs:/camadas/bronze/B_arqv.log  

4 - Após finalizar todos os passos acima, basta criar uma notebook Scala para iniciar as execuções dos código disponibilizado para leitura e transformação do arquivo. 
