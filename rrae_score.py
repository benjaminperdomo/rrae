# coding=UTF-8

import unicodedata
import ujson as json
from pyspark.sql.types import StructType, StructField, StringType, LongType, FloatType

from sparkcc import CCSparkJob
from bs4 import BeautifulSoup
from bs4.dammit import EncodingDetector

from palabrasclave import *

import requests
import os
import boto3


class RraeScoreJob(CCSparkJob):
    """ Procesamiento del indice RRAE: Sentimiento"""
    name = "RRAEScore"

    output_schema = StructType([
        StructField("key", StructType([
            StructField("name", StringType(), True),
            StructField("type", StringType(), True)]), True),
        StructField("cnt", LongType(), True)
    ])

    def process_record(self, record):
        ambiente = False
        nutresa = False
        quala = False
        ardila = False
        alpina = False

        # Configuración AWS

        if record.rec_type == 'response':
            # WARC response record
            contentType = record.http_headers.get_header('content-type', None)
            if contentType is not None and 'html' in contentType:
                # WARC es HTML
                payload = record.rec_headers.get_header(
                    'WARC-Identified-Payload-Type')
                if payload is not None and 'html' in payload:
                    html = record.content_stream().read()
                    titulo = self.get_html_title(html, record)
                    pagina = self.get_html_text(html, record)
                    titulominusculas = titulo.lower()

                    # Detectar palabras claves en título
                    for clave in claveAmbiente:
                        if(clave.lower() in titulominusculas):
                            ambiente = True

                    if ambiente:
                        # Detectar palabras conglomerado en título
                        for clave in claveAlpina:
                            if(clave.lower() in titulominusculas):
                                alpina = True

                        for clave in claveArdila:
                            if(clave.lower() in titulominusculas):
                                ardila = True

                        for clave in claveNutresa:
                            if(clave.lower() in titulominusculas):
                                nutresa = True

                        for clave in claveQuala:
                            if(clave.lower() in titulominusculas):
                                quala = True

                        # Contador match conglomerado - texto
                        if (alpina):
                            yield ("Alpina", "Total"), 1
                        if (ardila):
                            yield ("Ardila", "Total"), 1
                        if (nutresa):
                            yield ("Nutresa", "Total"), 1
                        if (quala):
                            yield ("Quala", "Total"), 1

                        # Sentimiento
                        evaluarSentimiento = nutresa or quala or ardila or alpina

                        if evaluarSentimiento:
                            sentimientoAzureTitulo = self.procesarSentimientoAzure(
                                titulo)
                            sentimientoAzurePagina = self.procesarSentimientoAzure(
                                pagina)
                            sentimientoAWSTitulo = self.procesarSentimientoAWS(
                                titulo)
                            sentimientoAWSPagina = self.procesarSentimientoAWS(
                                pagina)
                            if (alpina):
                                yield ("Alpina", "Azure Titulo " + sentimientoAzureTitulo[0]), 1
                                yield ("Alpina", "AWS Titulo " + sentimientoAWSTitulo[0]), 1
                                yield ("Alpina", "Azure Pagina " + sentimientoAzurePagina[0]), 1
                                yield ("Alpina", "AWS Pagina " + sentimientoAWSPagina[0]), 1
                            if (ardila):
                                yield ("Ardila", "Azure Titulo " + sentimientoAzureTitulo[0]), 1
                                yield ("Ardila", "AWS Titulo " + sentimientoAWSTitulo[0]), 1
                                yield ("Ardila", "Azure Pagina " + sentimientoAzurePagina[0]), 1
                                yield ("Ardila", "AWS Pagina " + sentimientoAWSPagina[0]), 1
                            if (nutresa):
                                yield ("Nutresa", "Azure Titulo " + sentimientoAzureTitulo[0]), 1
                                yield ("Nutresa", "AWS Titulo " + sentimientoAWSTitulo[0]), 1
                                yield ("Nutresa", "Azure Pagina " + sentimientoAzurePagina[0]), 1
                                yield ("Nutresa", "AWS Pagina " + sentimientoAWSPagina[0]), 1
                            if (quala):
                                yield ("Quala", "Azure Titulo " + sentimientoAzureTitulo[0]), 1
                                yield ("Quala", "AWS Titulo " + sentimientoAWSTitulo[0]), 1
                                yield ("Quala", "Azure Pagina " + sentimientoAzurePagina[0]), 1
                                yield ("Quala", "AWS Pagina " + sentimientoAWSPagina[0]), 1
                        else:
                            return
                    else:
                        return
                else:
                    return
        else:
            # WAT, warcinfo, request, non-WAT metadata records
            return

    def get_html_title(self, page, record):
        try:
            encoding = EncodingDetector.find_declared_encoding(
                page, is_html=True)
            soup = BeautifulSoup(page, "lxml", from_encoding=encoding)
            title = soup.title.string.strip()
            return title
        except:
            return ""

    def get_html_text(self, page, record):
        try:
            encoding = EncodingDetector.find_declared_encoding(
                page, is_html=True)
            soup = BeautifulSoup(page, "lxml", from_encoding=encoding)
            for script in soup(["script", "style"]):
                script.extract()
            return soup.get_text(" ", strip=True)
        except:
            return ""

    def procesarSentimientoAzure(self, texto):
        # Configuración Azure
        subscription_key = "AZURE_SUBSCRIPTION_KEY"
        endpoint = "AZURE_ENDPOINT"
        sentiment_url = endpoint + "/text/analytics/v3.0-preview.1/sentiment"
        maxText = texto[:5120]

        azurePeticion = {"documents": [
            {"id": "1", "language": "es",
                "text": maxText},
        ]}

        headers = {"Ocp-Apim-Subscription-Key": subscription_key}
        respuesta = requests.post(
            sentiment_url, headers=headers, json=azurePeticion)
        sentiment = respuesta.json()
        return sentiment['documents'][0]['sentiment'], sentiment['documents'][0]['documentScores']['positive'], sentiment['documents'][0]['documentScores']['neutral'], sentiment['documents'][0]['documentScores']['negative']

    def procesarSentimientoAWS(self, texto):
        comprehend = boto3.client(
            service_name='comprehend', region_name='us-east-1')
        maxText = str(texto.encode("utf-8")[:5000], "utf-8", errors="ignore")
        sentiment = comprehend.detect_sentiment(
            Text=maxText, LanguageCode='es')
        return sentiment['Sentiment'], sentiment['SentimentScore']['Mixed'], sentiment['SentimentScore']['Positive'], sentiment['SentimentScore']['Neutral'], sentiment['SentimentScore']['Negative']


if __name__ == "__main__":
    job = RraeScoreJob()
    job.run()
