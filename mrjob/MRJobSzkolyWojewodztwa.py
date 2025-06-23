from mrjob.job import MRJob
import csv
import json
from io import StringIO
import sys
from mrjob.protocol import RawValueProtocol, JSONValueProtocol, JSONProtocol
import logging
log = logging.getLogger(__name__)

logging.basicConfig(
    filename='debug.log',
    level=logging.DEBUG,
    format='[%(levelname)s] %(message)s'
)

INPUT_PROTOCOL = RawValueProtocol
OUTPUT_PROTOCOL = JSONProtocol
INTERNAL_PROTOCOL = JSONProtocol

#1
class MRJobUczniowie_w_Zawodach(MRJob):

    def mapper(self, _, line):
        line = line.decode("utf-8") if isinstance(line, bytes) else line

        if line.startswith("rok szkolny") or not line.strip():
            return

        try:
            fields = next(csv.reader(StringIO(line)))

            zawod = fields[33].strip().lower()

            key = zawod

            uczniowie = int(float(fields[36])) if len(fields) > 36 and fields[36] else 0
            dziewczeta = int(float(fields[37])) if len(fields) > 37 and fields[37] else 0
            chlopcy = uczniowie - dziewczeta

            yield key, {
                "uczniowie": uczniowie,
                "chlopcy" : chlopcy,
                "dziewczeta": dziewczeta,
            }

        except Exception as e:
            pass

    def reducer(self, key, values):
        suma = {
            "uczniowie": 0,
            "chlopcy": 0,
            "dziewczeta": 0
        }
        for val in values:
            for k in suma:
                suma[k] += val.get(k, 0)

        yield key, suma

#2
class MRJobTypObszaru_Zawody_LiczbaUczniow(MRJob):

    def mapper(self, _, line):
        line = line.decode("utf-8") if isinstance(line, bytes) else line

        if line.startswith("rok szkolny") or not line.strip():
            return

        try:
            fields = next(csv.reader(StringIO(line)))

            typ_obszaru = fields[6].strip().lower()
            zawod = fields[33].strip().lower()

            key = (typ_obszaru, zawod)

            uczniowie = int(float(fields[36])) if len(fields) > 36 and fields[36] else 0
            dziewczeta = int(float(fields[37])) if len(fields) > 37 and fields[37] else 0
            chlopcy = uczniowie - dziewczeta
            
            yield key, {
                "uczniowie": uczniowie,
                "dziewczeta": dziewczeta,
                "chlopcy": chlopcy
            }

        except Exception as e:
            pass

    def reducer(self, key, values):
        suma = {
            "uczniowie": 0,
            "dziewczeta": 0,
            "chlopcy": 0
        }
        for val in values:
            for k in suma:
                suma[k] += val.get(k, 0)

        yield key, suma

#3
class MRJobSzkoly_Wojewodztwa(MRJob):

    def mapper(self, _, line):
        line = line.decode("utf-8") if isinstance(line, bytes) else line

        if line.startswith("rok szkolny") or not line.strip():
            return

        try:
            fields = next(csv.reader(StringIO(line)))

            wojewodztwo = fields[3].strip().lower()
            regon = fields[31].strip()

            if wojewodztwo and regon:
                yield wojewodztwo, regon

        except Exception:
            pass

    def reducer(self, key, values):
        unikalne_regony = set(values)
        yield key, len(unikalne_regony)

#4
class MRJobSzkoly_TypPodmiotu_Wojewodztwa(MRJob):

    def mapper(self, _, line):
        line = line.decode("utf-8") if isinstance(line, bytes) else line

        if line.startswith("rok szkolny") or not line.strip():
            return

        try:
            fields = next(csv.reader(StringIO(line)))

            wojewodztwo = fields[3].strip().lower()
            typ_podmiotu = fields[10].strip().lower()
            key = (wojewodztwo, typ_podmiotu)

            yield key, 1

        except Exception:
            pass
        

    def reducer(self, key, values):
        yield key, sum(values)

#5        
class MRJobSzkoly_Publicznosc_Wojewodztwa(MRJob):

    def mapper(self, _, line):
        line = line.decode("utf-8") if isinstance(line, bytes) else line

        if line.startswith("rok szkolny") or not line.strip():
            return

        try:
            fields = next(csv.reader(StringIO(line)))

            wojewodztwo = fields[3].strip().lower()
            typ_podmiotu = fields[16].strip().lower()
            key = (wojewodztwo, typ_podmiotu)

            yield key, 1

            yield wojewodztwo, 1

        except Exception:
            pass

    def reducer(self, key, values):

        yield key, sum(values)

#6
class MRJobWojewodztwo_LiczbaUczniow(MRJob):

    def mapper(self, _, line):
        line = line.decode("utf-8") if isinstance(line, bytes) else line

        if line.startswith("rok szkolny") or not line.strip():
            return

        try:
            fields = next(csv.reader(StringIO(line)))

            wojewodztwo = fields[3].strip().lower()

            uczniowie = int(float(fields[36])) if len(fields) > 36 and fields[36] else 0
            
            yield wojewodztwo, {
                "uczniowie": uczniowie,
            }

        except Exception:
            pass
        

    def reducer(self, key, values):
        total = 0
        for val in values:
            total += val.get("uczniowie", 0)
        yield key, total

#7
class MRJobRok_LiczbaUczniow(MRJob):

    def mapper(self, _, line):
        line = line.decode("utf-8") if isinstance(line, bytes) else line

        if line.startswith("rok szkolny") or not line.strip():
            return

        try:
            fields = next(csv.reader(StringIO(line)))

            rok = "rok"

            uczniowie = int(float(fields[36])) if len(fields) > 36 and fields[36] else 0
            
            yield rok, {
                "uczniowie": uczniowie,
            }

        except Exception:
            pass
        

    def reducer(self, key, values):
        total = 0
        for val in values:
            total += val.get("uczniowie", 0)
        yield key, total
        

#7
class MRJobRok_LiczbaSzkół(MRJob):


    def mapper(self, _, line):
        line = line.decode("utf-8") if isinstance(line, bytes) else line

        if line.startswith("rok szkolny") or not line.strip():
            return

        try:
            fields = next(csv.reader(StringIO(line)))
            
            rok = "rok"

            regon = fields[31].strip()

            if rok and regon:
                yield rok, regon

        except Exception:
            pass

    def reducer(self, key, values):
        unikalne_regony = set(values)
        yield key, len(unikalne_regony)