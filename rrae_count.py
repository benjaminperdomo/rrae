# coding=UTF-8

import unicodedata
import ujson as json
from pyspark.sql.types import StructType, StructField, StringType, LongType

from sparkcc import CCSparkJob
from bs4 import BeautifulSoup
from bs4.dammit import EncodingDetector

from palabrasclave import *


class RraeCountJob(CCSparkJob):
    """ Procesamiento del indice RRAE: Conteo"""
    name = "RRAEConteo"

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
                    titulominusculas = titulo.lower()

                    # Detectar palabras claves en título
                    for clave in claveAmbiente:
                        if(clave.lower() in titulominusculas):
                            ambiente = True
                            yield ("Ambiente clave", clave), 1

                    # Detectar palabras conglomerado en título
                    for clave in claveAlpina:
                        if(clave.lower() in titulominusculas):
                            alpina = True
                            yield ("Alpina clave", clave), 1
                            if ambiente:
                                yield ("Ambiente Alpina clave", clave), 1

                    for clave in claveArdila:
                        if(clave.lower() in titulominusculas):
                            ardila = True
                            yield ("Ardila clave", clave), 1
                            if ambiente:
                                yield ("Ambiente Ardila clave", clave), 1

                    for clave in claveNutresa:
                        if(clave.lower() in titulominusculas):
                            nutresa = True
                            yield ("Nutresa clave", clave), 1
                            if ambiente:
                                yield ("Ambiente Nutresa clave", clave), 1

                    for clave in claveQuala:
                        if(clave.lower() in titulominusculas):
                            quala = True
                            yield ("Quala clave", clave), 1
                            if ambiente:
                                yield ("Ambiente Quala clave", clave), 1

                    # Contador match conglomerado - texto
                    if ambiente:
                        yield ("Ambiente", "Total"), 1
                        if (alpina):
                            yield ("Ambiente Alpina", "Total"), 1
                        if (ardila):
                            yield ("Ambiente Ardila", "Total"), 1
                        if (nutresa):
                            yield ("Ambiente Nutresa", "Total"), 1
                        if (quala):
                            yield ("Ambiente Quala", "Total"), 1
                    else:
                        yield ("No ambiente", "Total"), 1

                    if (alpina):
                        yield ("Alpina", "Total"), 1
                    if (ardila):
                        yield ("Ardila", "Total"), 1
                    if (nutresa):
                        yield ("Nutresa", "Total"), 1
                    if (quala):
                        yield ("Quala", "Total"), 1

                else:
                    yield ("No procesado", "Total"), 1
        else:
            # WAT, warcinfo, request, non-WAT metadata records
            yield ("No procesado", "Total"), 1

    def get_html_title(self, page, record):
        try:
            encoding = EncodingDetector.find_declared_encoding(
                page, is_html=True)
            soup = BeautifulSoup(page, "lxml", from_encoding=encoding)
            title = soup.title.string.strip()
            return title
        except:
            return ""


if __name__ == "__main__":
    job = RraeCountJob()
    job.run()
