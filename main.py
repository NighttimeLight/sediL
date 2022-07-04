
from xml.etree.ElementTree import parse
import pandas as pd

# from pyspark.sql import SparkSession
# spark = SparkSession.builder.getOrCreate()

pathPref = ''

def parseDniWolneToDF(xmlStr):
    xml = parse(xmlStr)
    dziens = []
    for data in xml.iterfind('DATA'):
        dziens.append(data.findtext('DZIEN'))
    return pd.DataFrame({'DZIEN': dziens})



sklepyDF = pd.read_csv(pathPref+'ExternalData/sklepy.txt', encoding='windows-1250')
dni_wolneDF = parseDniWolneToDF(pathPref+'ExternalData/dni_wolne.xml')

centralSprzedazDF = pd.read_csv(pathPref+'ExternalDatabases/central_etl_sprzedaz.csv', encoding='windows-1250')
eastSprzedazDF = pd.read_csv(pathPref+'ExternalDatabases/east_etl_sprzedaz.csv', encoding='windows-1250')
westSprzedazDF = pd.read_csv(pathPref+'ExternalDatabases/west_etl_sprzedaz.csv', encoding='windows-1250')
allSprzedazDF = pd.concat([centralSprzedazDF, eastSprzedazDF, westSprzedazDF], ignore_index=True, sort=False)

allDatyDF = allSprzedazDF.drop_duplicates()

centralProduktyDF = pd.read_csv(pathPref+'ExternalDatabases/central_etl_produkty.csv', encoding='windows-1250')
eastProduktyDF = pd.read_csv(pathPref+'ExternalDatabases/east_etl_produkty.csv', encoding='windows-1250')
westProduktyDF = pd.read_csv(pathPref+'ExternalDatabases/west_etl_produkty.csv', encoding='windows-1250')
allProduktyDF = pd.concat([centralProduktyDF, eastProduktyDF, westProduktyDF], ignore_index=True, sort=False).\
    drop_duplicates()

centralKategorieDF = pd.read_csv(pathPref+'ExternalDatabases/central_etl_kategorie.csv', encoding='windows-1250')
eastKategorieDF = pd.read_csv(pathPref+'ExternalDatabases/east_etl_kategorie.csv', encoding='windows-1250')
westKategorieDF = pd.read_csv(pathPref+'ExternalDatabases/west_etl_kategorie.csv', encoding='windows-1250')
allKategorieDF = pd.concat([centralKategorieDF, eastKategorieDF, westKategorieDF], ignore_index=True, sort=False).\
    drop_duplicates()

centralDepartamentyDF = pd.read_csv(pathPref+'ExternalDatabases/central_etl_departamenty.csv', encoding='windows-1250')
eastDepartamentyDF = pd.read_csv(pathPref+'ExternalDatabases/east_etl_departamenty.csv', encoding='windows-1250')
westDepartamentyDF = pd.read_csv(pathPref+'ExternalDatabases/west_etl_departamenty.csv', encoding='windows-1250')
allDepartamentyDF = pd.concat([centralDepartamentyDF, eastDepartamentyDF, westDepartamentyDF], ignore_index=True, sort=False).\
    drop_duplicates()



tempDatyDF = allDatyDF.merge(dni_wolneDF, how='left', left_on='s_data', right_on='DZIEN')
datyDF = tempDatyDF.\
    assign(rok=lambda x: x.s_data.str[:4].astype(int)).\
    assign(miesiac=lambda x: x.s_data.str[5:7].astype(int)).\
    assign(data=lambda x: pd.to_datetime(x.s_data).dt.date).\
    assign(czy_wolny=lambda x: x.DZIEN.notna()).\
    drop(['DZIEN', 's_data'], axis=1)

tempProduktyDF = allProduktyDF.\
    merge(allKategorieDF, how='left', left_on='p_k_id', right_on='k_id').\
    merge(allDepartamentyDF, how='left', left_on='p_d_id', right_on='d_id')
typyDF = pd.DataFrame({'t_id': [1, 2, 3], 'typ': ['Filmy', 'Gry', 'Zywnosc']})
produktyDF = tempProduktyDF.\
    merge(typyDF, how='left', left_on='p_t_id', right_on='t_id').\
    assign(id_produktu=lambda x: x.p_id.astype(int)).\
    drop(['p_k_id', 'p_d_id', 'p_t_id', 'k_id', 'd_id', 'p_id', 't_id'], axis=1).\
    rename(columns={'p_nazwa': 'nazwa_produktu',
                    'k_nazwa': 'nazwa_kategorii',
                    'd_nazwa': 'nazwa_departamentu',
                    })

sklepyDF = sklepyDF.\
    rename(columns={'SK_REGION': 'region',
                    'SK_MIEJSCOWOSC': 'miasto',
                    'SK_NAZWA': 'sklep',
                    'SK_ID': 'id_sklepu',
                    'SK_REGION': 'region',
                    })\
    [['region', 'miasto', 'sklep', 'id_sklepu']]

sprzedazDF = allSprzedazDF.\
    assign(cena_sprzedazy=lambda x: x.s_cena_sprzedazy.astype(float)).\
    assign(cena_zakupu=lambda x: x.s_cena_zakupu.astype(float)).\
    assign(liczba_towarow=lambda x: x.s_liczba_towarow.astype(int)).\
    assign(liczba_klientow=lambda x: x.s_liczba_klientow.astype(int)).\
    assign(id_sklepu=lambda x: x.s_sk_id.astype(int)).\
    assign(data=lambda x: pd.to_datetime(x.s_data).dt.date).\
    assign(id_produktu=lambda x: x.s_p_id.astype(int)).\
    drop(['s_data', 's_sk_id', 's_p_id', 's_cena_sprzedazy', 's_cena_zakupu',
          's_liczba_towarow', 's_liczba_klientow'], axis=1)

print(datyDF.head())
print(produktyDF.head())
print(sklepyDF.head())
print(sprzedazDF.head())

