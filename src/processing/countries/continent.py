from src.common_imports import *
def continent_fun(id):
    continent = substring(id,1,2)
    return when(continent == "11", lit("Europe")) \
           .when(continent == "22", lit("Africa")) \
           .when(continent == "33", lit("Asia")) \
           .when(continent == "44", lit("Australia and Oceania")) \
           .when(continent == "55", lit("North America")) \
           .when(continent == "66", lit("South America")) \
           .when(continent == "13", lit("Transcontinental (Europe/Asia)")) \
           .when(continent == "10", lit("Europe/Global Territories (UK, France)")) \
           .when(continent == "50", lit("North America/Global Territories (USA)")) \
           .otherwise(lit("UNKNOWN"))
