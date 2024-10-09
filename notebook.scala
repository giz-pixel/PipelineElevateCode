//########################################################################################################################################

import org.apache.spark.sql.functions.regexp_extract
import org.apache.spark.sql.functions._ 
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.functions.broadcast
import io.delta.tables._

//###############################################################-BLOCO 1-################################################################
                                      //##################-GERAR UM DF ESTRUTURADO-##################


/*
  *** Antes de executar os blocos abaixo, favor, importar as bibliotecas do bloco anterior
  
  O metodo abaixo (getbronze) foi desenvolvido para extrair as informações do arquivo de log.
  O objetivo desta extração foi obter trechos do arquivo para esttuturar um dataframe final.
  Este dataframe final possui as seguintes colunas: IP, ENDPOINTS, BYTES, ACCESS_TIME, ACCESS_DATE, DAY_WEEK, STATUS.

  Ao executar o código abaixo, é importante seguir a ordem descrita neste documento, contida em cada bloco.

*/                                      

def getbronze(spark: SparkSession): DataFrame = {

  val pegaURL = """(https?://\S+)"""
  var dfbronze = spark.read.option("header", "false").textFile("dbfs:/camadas/bronze/B_arqv.log")

val df1 =  dfbronze.withColumn("pegaURL", regexp_extract(col("value"), pegaURL.toString,1))					
					        .select(
                     		col("pegaURL"),                                               
					        	    regexp_extract($"value", """^(\S+)""",1).as("IP"),
					        	    regexp_extract($"value", """^\S+ (\S+)""",1).as("identd"),
					        	    regexp_extract($"value", """^\S+ \S+ (\S+)""",1).as("user"),
					        	    regexp_extract($"value", """\[(.*?)]""",1).as("timestemp"),
					        	    regexp_extract($"value", """\"(.*?)\"""",1).as("request"),
					        	    regexp_extract($"value", """\".*\" (\d{3})""",1).as("status"),
					        	    regexp_extract($"value", """\".*\" \d{3} (\S+)""",1).as("bytes"),
					        	    regexp_extract($"value", """\".*\" \d{3} \d+ \"[^\"]*\" \"(.*?)\"""",1).as("user_agent"),
					        	    regexp_extract($"value", """\".*\" \d{3} \d+ \"(.*?)\"""",1).as("referrer_inter")
					        	)                        


val arqLog = df1.withColumn("FORMATAR_DATA", to_timestamp(col("timestemp"),"dd/MMM/yyyy:HH:mm:ss Z"))
                 .withColumn("ACCESS_TIME", date_format(col("FORMATAR_DATA"), "HH:mm"))	
                 .withColumn("ACCESS_DATE", date_format(to_date(col("FORMATAR_DATA")), "yyyy-MM-dd"))
                 .withColumn("DAY_WEEK", date_format(col("FORMATAR_DATA"), "EEEE"))
                 .withColumn("ENDPOINTS", regexp_replace(col("pegaURL"), "[\")]", ""))
                 .select(                     
                    col("IP"), 
                    col("ENDPOINTS"),
                    col("BYTES"),
                    col("ACCESS_TIME"),
                    col("ACCESS_DATE"), 
                    col("DAY_WEEK"),
                    col("STATUS")
                        ) 
                        
	arqLog						
}

//esta variável armazenará o dataframe que será utilizado nos próximos passos
var bronze = getbronze(spark)

//###############################################################-BLOCO 2-################################################################
                                      //##################-LIMPEZA DOS DADOS-##################


/*
  A partir do dataframe principal (bronze), foi realizado alguns tratamentos como: 
  - foi gerado um novo dataframe para realizar o filtros abaixo:
  - filtrado endpoints com registros diferentes de branco 
  - filtrado os registros diferentes de zero(0), para evitar erros nos calculos futuro
  - remoção de espaços nas colunas 

*/                                      

val dfTratado = bronze.filter((col("ENDPOINTS") =!= "") && (col("BYTES") =!= lit("0")))            
        .select(         
          trim(col("IP")).as("IP"), 
          trim(col("ENDPOINTS")).as("ENDPOINTS"), 
          trim(col("BYTES")).as("BYTES"), 
          trim(col("ACCESS_TIME")).as("ACCESS_TIME"), 
          trim(col("ACCESS_DATE")).as("ACCESS_DATE"), 
          trim(col("DAY_WEEK")).as("DAY_WEEK"), 
          trim(col("STATUS")).cast(IntegerType).as("STATUS")
          ).distinct()

//###############################################################-BLOCO 3-################################################################
                                      //##################-GRAVAR O DF EM UMA TABELA-##################

/* 
  Depois de executar a limpeza dos dados, seguiremos com a gravação do dataframe como uma tabela delta. 
  A variável dat armazena a data atual, ela foi criada para gerar uma nova coluna chamada DT_REFE que será usada como partição.

  Após a gravação será fonecido uma tabela chamada: TB_ARQUIVO_LOG_SILVER

*/

val dat = current_date()
val df = dfTratado.filter(col("BYTES") =!= "0").withColumn("DT_REFE", lit(dat))
val datAtual = df.select(col("DT_REFE")).take(1)(0).get(0)

df.write
  .format("delta")
  .mode("overwrite")
  .partitionBy("DT_REFE")
  .option("path", "dbfs:/camadas/silver")
  .saveAsTable("TB_ARQUIVO_LOG_SILVER")

//Após a gravação, é possível ler a tabela, por meio do spark table, de acordo abaixo: 
var dfSilver = spark.table("TB_ARQUIVO_LOG_SILVER")

//###############################################################-BLOCO 4-################################################################
                                      //##################-RESPONDENDO AOS DESAFIOS-##################
/*
Após o processamento/tratamento dos desafios, será gerado uma tabela em formato delta na camada gold.
Para isso, é importante realizar a leitura da tabela TB_ARQUIVO_LOG_SILVER e a partir dela gerar um dataframe com as transformações necessárias
*/

//leitura da tabela da camada silver para responder aos desafios abaixo: 
var dfSilver = spark.table("TB_ARQUIVO_LOG_SILVER")



//1. **Identifique as 10 maiores origens de acesso (Client IP) por quantidade de acessos.**
var d1 = dfSilver.groupBy(col("IP")).agg(count("*").as("TOP10_ORIG_ACCESS_IP"), first("DT_REFE").as("DT_REFE"))
var d1Fin = d1.orderBy(desc("TOP10_ORIG_ACCESS_IP")).limit(10)

d1Fin.write
     .format("delta")
     .mode("overwrite")
     .partitionBy("DT_REFE")
     .option("path", "dbfs:/camadas/gold")
     .option("overwriteSchema", "true")
     .saveAsTable("TB_TOP10_ORIG_ACCESS_GOLD")


//A partir do trecho acima foi gerado a tabela a seguir
var top10 = spark.table("TB_TOP10_ORIG_ACCESS_GOLD").show()

 ########################################################################################################################################


//2. **Liste os 6 endpoints mais acessados, desconsiderando aqueles que representam arquivos.**
val clearExt = """\.(jpeg|png|jpg|gif|txt|zip|css|js|ico|svg)"""

var d2 = dfSilver.withColumn("extensões_arq", col("ENDPOINTS").rlike(clearExt))
var removeArq = d2.filter((col("extensões_arq").notEqual(true)) && (col("ENDPOINTS") =!= "")).drop("extensões_arq") 
var getEndPoints = removeArq.groupBy(col("ENDPOINTS")).agg(count("*").as("TOP6_ACCESS"), first("DT_REFE").as("DT_REFE"))
var dfFin = getEndPoints.orderBy(desc("TOP6_ACCESS")).limit(6)

dfFin.write
     .format("delta")
     .mode("overwrite")
     .partitionBy("DT_REFE")
     .option("path", "dbfs:/camadas/gold")
     .option("overwriteSchema", "true")
     .saveAsTable("TB_TOP6_ACCESS_GOLD")

 
 //A tabela final poderá ser acessada de acordo abaixo:
 var top6 = spark.table("TB_TOP6_ACCESS_GOLD").show()



//3. **Qual a quantidade de Client IPs distintos?**
var d3Orig = dfSilver.select(col("IP"), col("DT_REFE")).distinct
var d3Distinct = d3Orig.select(count("IP").as("QTD_IP_DISTINCT"), first("DT_REFE").as("DT_REFE"))

d3Distinct.write
          .format("delta")
          .mode("overwrite")
          .partitionBy("DT_REFE")
          .option("path", "dbfs:/camadas/gold")
          .option("overwriteSchema", "true")
          .saveAsTable("TB_QTD_IP_DISTINCT_GOLD")

//Tabela do desafio acima
spark.table("TB_QTD_IP_DISTINCT_GOLD").show()



//4. **Quantos dias de dados estão representados no arquivo?**
var df4 = dfSilver.agg(
                  max(col("ACCESS_DATE")).as("START_DATE"),
                  min(col("ACCESS_DATE")).as("END_DATE"),
                  first("DT_REFE").as("DT_REFE")
                  ).select(                    
                    datediff(col("START_DATE"), col("END_DATE")).as("DIFF_DATE"),
                    col("DT_REFE")
                  )

df4.write
   .format("delta")
   .mode("overwrite")
   .partitionBy("DT_REFE")
   .option("path", "dbfs:/camadas/gold")
   .option("overwriteSchema", "true")
   .saveAsTable("TB_QTD_DATA_DAY_GOLD")     


// A tabela poderá ser acessada em:
val daydiff = spark.table("TB_QTD_DATA_DAY_GOLD").show()



//5. **Com base no tamanho (em bytes) do conteúdo das respostas, faça a seguinte análise:**
// - *Dica:* Considere como os dados podem ser categorizados por tipo de resposta para realizar essas análises.
 var category = dfSilver.withColumn("RESPONSE_TYPE", 
                                        when(col("STATUS") === lit("200"), lit("(200) - SUCESSO"))
                                        .when(col("STATUS").isin("404"), lit("(404) - ERRO_DO_CLIENTE"))                                        
                                        .when(col("STATUS").isin("301"), lit("(301) - REDIRECIONAMENTO"))
                                        .when(col("STATUS").isin("304"), lit("(304) - DADO ARMAZENADO EM CACHE"))
                                        .when(col("STATUS").isin("302"), lit("(302) - URL TEMPORARIO"))
                                       ) 

 var dfvolumndata =  category.groupBy("RESPONSE_TYPE")
                    .agg(                          
                      sum(col("BYTES").cast("int")).as("VOLUM_TOTAL"), //O volume total de dados retornado.
                      max(col("BYTES").cast("int")).as("MAX_VOLUM"), //O maior volume de dados em uma única resposta.
                      min(col("BYTES").cast("int")).as("MIN_VOLUM"), //O menor volume de dados em uma única resposta.
                      avg(col("BYTES").cast("Decimal(36,2)")).as("MEDIA_VOLUM"), //O volume médio de dados retornado.
                      first("DT_REFE").as("DT_REFE")
                        )

dfvolumndata.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("DT_REFE")
            .option("path", "dbfs:/camadas/gold") 
            .option("overwriteSchema", "true")
            .saveAsTable("TB_SIZE_BYTES_RESPONSE_GOLD")

//Table abaixo:
val sizebytes = spark.table("TB_SIZE_BYTES_RESPONSE_GOLD").show()      


//6. **Qual o dia da semana com o maior número de erros do tipo "HTTP Client Error"?**
var df = dfSilver.withColumn("RESPONSE_TYPE", 
                        when(col("STATUS") === lit("200"), lit("(200) - SUCESSO"))
                        .when(col("STATUS").isin("404"), lit("(404)-ERRO_DO_CLIENTE"))
                        .when(col("STATUS").isin("500"), lit("(500) - ERRO_DO_SERVIDOR"))
                        .when(col("STATUS").isin("301"), lit("(301) - REDIRECIONAMENTO"))
                    )

var getError = df.filter(col("RESPONSE_TYPE").equalTo("(404)-ERRO_DO_CLIENTE"))   

var getDayError = getError.groupBy(col("DAY_WEEK").as("DAY_WEEK_ERROR"),col("DT_REFE")).count().orderBy(desc("count"))

getDayError.write
          .format("delta")
          .mode("overwrite")
          .partitionBy("DT_REFE")
          .option("path", "dbfs:/camadas/gold")
          .option("overwriteSchema", "true")
          .saveAsTable("TB_DAY_WEEK_ERROR_GOLD")

var dayweekerror = spark.table("TB_DAY_WEEK_ERROR_GOLD").show()        

//######################################################################################################################################
                         //##################-AO FINAL DAS EXECUÇÕES SERÁ GERADO AS TABELAS ABAIXO-##################

var dfSilver = spark.table("TB_ARQUIVO_LOG_SILVER") 
var top10 = spark.table("TB_TOP10_ORIG_ACCESS_GOLD") 
var top6 = spark.table("TB_TOP6_ACCESS_GOLD") 
val daydiff = spark.table("TB_QTD_DATA_DIFFDAY_GOLD") 
val sizebytes = spark.table("TB_SIZE_BYTES_RESPONSE_GOLD") 




