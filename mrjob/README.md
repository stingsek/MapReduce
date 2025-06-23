# data source

https://dane.gov.pl/pl/dataset/1617,uczniowie-wedlug-zawodow

# run python env

```
python3 -m venv venv
source venv/bin/activate  
pip install -r requirements.txt
```

# install your dependencies before push
```
pip freeze > requirements.txt
```

# hadoop run
```bash
stop-yarn.sh
stop-dfs.sh
start-dfs.sh
start-yarn.sh
```

YARN ResourceManager: http://localhost:8088
HDFS NameNode UI: http://localhost:9870

The project supports three execution modes:

    inline -Runs as a regular single-threaded Python script. Ideal for unit testing and debugging.

    local - Executes multiple Python processes in parallel on the local machine. Useful for testing MapReduce logic without a full Hadoop setup.

    hadoop - Executes on a full Hadoop MapReduce infrastructure:
      Submits a job to the Hadoop cluster.
      Allocates resources via YARN.
      Creates temporary files (staging directories, job.jar, .tmp, output folders).
      Launches containers for mappers and reducers.
      Uses HDFS as the input/output system instead of the local filesystem.

columns: 
rok szkolny,idTerytGmina,idTerytWojewodztwo,Wojewodztwo,Powiat,Gmina,Typ obszaru,Miejscowosc,RSPO,idTypPodmiotu,Typ podmiotu,idRodzajPlacowki,Rodzaj szkoly/placowki,idKategoriaUczniow,Kategoria uczniow,idPublicznosc,Publicznosc,idSpecyfikaSzkoly,Specyfika szkoly,idTypOrgProw,Typ organu prowadzacego,Organ prowadzacy,Nazwa placowki,Ulica,Numer domu,Kod pocztowy,Poczta,Numer telefonu,Numer faksu,Adres email,Adres www,regon,idZawod,Zawod,idKlasa,Klasa,liczba uczniow,w tym dziewczeta,liczba mlodocianych pracownikow,w tym dziewczeta_mlod prac,liczba absolwentow,w tym dziewczeta_abs

  [0] rok szkolny = 2023/2024
  [1] idTerytGmina = 201011
  [2] idTerytWojewodztwo = 2
  [3] Wojewodztwo = DOLNOŚLĄSKIE
  [4] Powiat = bolesławiecki
  [5] Gmina = Bolesławiec
  [6] Typ obszaru = obszar miejski
  [7] Miejscowosc = Bolesławiec
  [8] RSPO = 18924
  [9] idTypPodmiotu = 16
  [10] Typ podmiotu = Technikum
  [11] idRodzajPlacowki = 7
  [12] Rodzaj szkoly/placowki = szkoła/placówka wchodząca w skład jednostki złożonej
  [13] idKategoriaUczniow = 1
  [14] Kategoria uczniow = Dzieci lub młodzież
  [15] idPublicznosc = 1
  [16] Publicznosc = publiczna
  [17] idSpecyfikaSzkoly = 100
  [18] Specyfika szkoly = brak specyfiki
  [19] idTypOrgProw = 132
  [20] Typ organu prowadzacego = Powiat ziemski
  [21] Organ prowadzacy = jst
  [22] Nazwa placowki = TECHNIKUM NR 4 W BOLESŁAWCU
  [23] Ulica = ul. Heleny i Wincentego Tyrankiewiczów
  [24] Numer domu = 2
  [25] Kod pocztowy = 59-700
  [26] Poczta = Bolesławiec
  [27] Numer telefonu = 756494560
  [28] Numer faksu = 
  [29] Adres email = zse@poczta.internetdsl.pl
  [30] Adres www = zse.boleslawiec.pl
  [31] regon = 231183533
  [32] idZawod = 6
  [33] Zawod = Technik elektronik
  [34] idKlasa = 
  [35] Klasa = 
  [36] liczba uczniow = 
  [37] w tym dziewczeta = 
  [38] liczba absolwentow = 16