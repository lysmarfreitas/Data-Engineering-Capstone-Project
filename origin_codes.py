from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

#visas codes from I94_SAS_Lables_Description file converted to python dictionary then create a udf.

visa_codes = {
   1: 'Business',
   2: 'Pleasure',
   3: 'Student',
}

#create udf to return the country of origin
visa_code_udf=udf(lambda x: visa_codes[x],StringType())

#immigration codes from I94_SAS_Lables_Description file converted to python dictionary then create a udf.
country_codes = {
    582: 'MEXICO Air Sea, and Not Reported (I-94, no land arrivals)',
    236:  'AFGHANISTAN',
    101:  'ALBANIA',
    316:  'ALGERIA',
    102:  'ANDORRA',
    324:  'ANGOLA',
    529:  'ANGUILLA',
    518:  'ANTIGUA-BARBUDA',
    687:  'ARGENTINA ',
    151:  'ARMENIA',
    532:  'ARUBA',
    438:  'AUSTRALIA',
    103:  'AUSTRIA',
    152:  'AZERBAIJAN',
    512:  'BAHAMAS',
    298:  'BAHRAIN',
    274:  'BANGLADESH',
    513:  'BARBADOS',
    104:  'BELGIUM',
    581:  'BELIZE',
    386:  'BENIN',
    509:  'BERMUDA',
    153:  'BELARUS',
    242:  'BHUTAN',
    688:  'BOLIVIA',
    717:  'BONAIRE, ST EUSTATIUS, SABA',
    164:  'BOSNIA-HERZEGOVINA',
    336:  'BOTSWANA',
    689:  'BRAZIL',
    525:  'BRITISH VIRGIN ISLANDS',
    217:  'BRUNEI',
    105:  'BULGARIA',
    393:  'BURKINA FASO',
    243:  'BURMA',
    375:  'BURUNDI',
    310:  'CAMEROON',
    326:  'CAPE VERDE',
    526:  'CAYMAN ISLANDS',
    383:  'CENTRAL AFRICAN REPUBLIC',
    384:  'CHAD',
    690:  'CHILE',
    245:  'CHINA, PRC',
    721:  'CURACAO',
    270:  'CHRISTMAS ISLAND',
    271:  'COCOS ISLANDS',
    691:  'COLOMBIA',
    317:  'COMOROS',
    385:  'CONGO',
    467:  'COOK ISLANDS',
    575:  'COSTA RICA',
    165:  'CROATIA',
    584:  'CUBA',
    218:  'CYPRUS',
    140:  'CZECH REPUBLIC',
    723:  'FAROE ISLANDS (PART OF DENMARK)',
    108:  'DENMARK',
    322:  'DJIBOUTI',
    519:  'DOMINICA',
    585:  'DOMINICAN REPUBLIC',
    240:  'EAST TIMOR',
    692:  'ECUADOR',
    368:  'EGYPT',
    576:  'EL SALVADOR',
    399:  'EQUATORIAL GUINEA',
    372:  'ERITREA',
    109:  'ESTONIA',
    369:  'ETHIOPIA',
    604:  'FALKLAND ISLANDS',
    413:  'FIJI',
    110:  'FINLAND',
    111:  'FRANCE',
    601:  'FRENCH GUIANA',
    411:  'FRENCH POLYNESIA',
    387:  'GABON',
    338:  'GAMBIA',
    758:  'GAZA STRIP',
    154:  'GEORGIA',
    112:  'GERMANY',
    339:  'GHANA',
    143:  'GIBRALTAR',
    113:  'GREECE',
    520:  'GRENADA',
    507:  'GUADELOUPE',
    577:  'GUATEMALA',
    382:  'GUINEA',
    327:  'GUINEA-BISSAU',
    603:  'GUYANA',
    586:  'HAITI',
    726:  'HEARD AND MCDONALD IS.',
    149:  'HOLY SEE/VATICAN',
    528:  'HONDURAS',
    206:  'HONG KONG',
    114:  'HUNGARY',
    115:  'ICELAND',
    213:  'INDIA',
    759:  'INDIAN OCEAN AREAS (FRENCH)',
    729:  'INDIAN OCEAN TERRITORY',
    204:  'INDONESIA',
    249:  'IRAN',
    250:  'IRAQ',
    116:  'IRELAND',
    251:  'ISRAEL',
    117:  'ITALY',
    388:  'IVORY COAST',
    514:  'JAMAICA',
    209:  'JAPAN',
    253:  'JORDAN',
    201:  'KAMPUCHEA',
    155:  'KAZAKHSTAN',
    340:  'KENYA',
    414:  'KIRIBATI',
    732:  'KOSOVO',
    272:  'KUWAIT',
    156:  'KYRGYZSTAN',
    203:  'LAOS',
    118:  'LATVIA',
    255:  'LEBANON',
    335:  'LESOTHO',
    370:  'LIBERIA',
    381:  'LIBYA',
    119:  'LIECHTENSTEIN',
    120:  'LITHUANIA',
    121:  'LUXEMBOURG',
    214:  'MACAU',
    167:  'MACEDONIA',
    320:  'MADAGASCAR',
    345:  'MALAWI',
    273:  'MALAYSIA',
    220:  'MALDIVES',
    392:  'MALI',
    145:  'MALTA',
    472:  'MARSHALL ISLANDS',
    511:  'MARTINIQUE',
    389:  'MAURITANIA',
    342:  'MAURITIUS',
    760:  'MAYOTTE (AFRICA - FRENCH)',
    473:  'MICRONESIA, FED. STATES OF',
    157:  'MOLDOVA',
    122:  'MONACO',
    299:  'MONGOLIA',
    735:  'MONTENEGRO',
    521:  'MONTSERRAT',
    332:  'MOROCCO',
    329:  'MOZAMBIQUE',
    371:  'NAMIBIA',
    440:  'NAURU',
    257:  'NEPAL',
    123:  'NETHERLANDS',
    508:  'NETHERLANDS ANTILLES',
    409:  'NEW CALEDONIA',
    464:  'NEW ZEALAND',
    579:  'NICARAGUA',
    390:  'NIGER',
    343:  'NIGERIA',
    470:  'NIUE',
    275:  'NORTH KOREA',
    124:  'NORWAY',
    256:  'OMAN',
    258:  'PAKISTAN',
    474:  'PALAU',
    743:  'PALESTINE',
    504:  'PANAMA',
    441:  'PAPUA NEW GUINEA',
    693:  'PARAGUAY',
    694:  'PERU',
    260:  'PHILIPPINES',
    416:  'PITCAIRN ISLANDS',
    107:  'POLAND',
    126:  'PORTUGAL',
    297:  'QATAR',
    748:  'REPUBLIC OF SOUTH SUDAN',
    321:  'REUNION',
    127:  'ROMANIA',
    158:  'RUSSIA',
    376:  'RWANDA',
    128:  'SAN MARINO',
    330:  'SAO TOME AND PRINCIPE',
    261:  'SAUDI ARABIA',
    391:  'SENEGAL',
    142:  'SERBIA AND MONTENEGRO',
    745:  'SERBIA',
    347:  'SEYCHELLES',
    348:  'SIERRA LEONE',
    207:  'SINGAPORE',
    141:  'SLOVAKIA',
    166:  'SLOVENIA',
    412:  'SOLOMON ISLANDS',
    397:  'SOMALIA',
    373:  'SOUTH AFRICA',
    276:  'SOUTH KOREA',
    129:  'SPAIN',
    244:  'SRI LANKA',
    346:  'ST. HELENA',
    522:  'ST. KITTS-NEVIS',
    523:  'ST. LUCIA',
    502:  'ST. PIERRE AND MIQUELON',
    524:  'ST. VINCENT-GRENADINES',
    716:  'SAINT BARTHELEMY',
    736:  'SAINT MARTIN',
    749:  'SAINT MAARTEN',
    350:  'SUDAN',
    602:  'SURINAME',
    351:  'SWAZILAND',
    130:  'SWEDEN',
    131:  'SWITZERLAND',
    262:  'SYRIA',
    268:  'TAIWAN',
    159:  'TAJIKISTAN',
    353:  'TANZANIA',
    263:  'THAILAND',
    304:  'TOGO',
    417:  'TONGA',
    516:  'TRINIDAD AND TOBAGO',
    323:  'TUNISIA',
    264:  'TURKEY',
    161:  'TURKMENISTAN',
    527:  'TURKS AND CAICOS ISLANDS',
    420:  'TUVALU',
    352:  'UGANDA',
    162:  'UKRAINE',
    296:  'UNITED ARAB EMIRATES',
    135:  'UNITED KINGDOM',
    695:  'URUGUAY',
    163:  'UZBEKISTAN',
    410:  'VANUATU',
    696:  'VENEZUELA',
    266:  'VIETNAM',
    469:  'WALLIS AND FUTUNA ISLANDS',
    757:  'WEST INDIES (FRENCH)',
    333:  'WESTERN SAHARA',
    465:  'WESTERN SAMOA',
    216:  'YEMEN',
    139:  'YUGOSLAVIA',
    301:  'ZAIRE',
    344:  'ZAMBIA',
    315:  'ZIMBABWE',
    403:  'INVALID: AMERICAN SAMOA',
    712:  'INVALID: ANTARCTICA',
    700:  'INVALID: BORN ON BOARD SHIP',
    719:  'INVALID: BOUVET ISLAND (ANTARCTICA/NORWAY TERR.)',
    574:  'INVALID: CANADA',
    720:  'INVALID: CANTON AND ENDERBURY ISLS',
    106:  'INVALID: CZECHOSLOVAKIA',
    739:  'INVALID: DRONNING MAUD LAND (ANTARCTICA-NORWAY)',
    394:  'INVALID: FRENCH SOUTHERN AND ANTARCTIC',
    501:  'INVALID: GREENLAND',
    404:  'INVALID: GUAM',
    730:  'INVALID: INTERNATIONAL WATERS',
    731:  'INVALID: JOHNSON ISLAND',
    471:  'INVALID: MARIANA ISLANDS, NORTHERN',
    737:  'INVALID: MIDWAY ISLANDS',
    753:  'INVALID: MINOR OUTLYING ISLANDS - USA',
    740:  'INVALID: NEUTRAL ZONE (S. ARABIA/IRAQ)',
    710:  'INVALID: NON-QUOTA IMMIGRANT',
    505:  'INVALID: PUERTO RICO',
    0:  'INVALID: STATELESS',
    705:  'INVALID: STATELESS',
    583:  'INVALID: UNITED STATES',
    407:  'INVALID: UNITED STATES',
    999:  'INVALID: UNKNOWN',
    239:  'INVALID: UNKNOWN COUNTRY',
    134:  'INVALID: USSR',
    506:  'INVALID: U.S. VIRGIN ISLANDS',
    755:  'INVALID: WAKE ISLAND',
    311:  'Collapsed Tanzania (should not show)',
    741:  'Collapsed Curacao (should not show)',
    54:  'No Country Code (54)',
    100:  'No Country Code (100)',
    187:  'No Country Code (187)',
    190:  'No Country Code (190)',
    200:  'No Country Code (200)',
    219:  'No Country Code (219)',
    238:  'No Country Code (238)',
    277:  'No Country Code (277)',
    293:  'No Country Code (293)',
    300:  'No Country Code (300)',
    319:  'No Country Code (319)',
    365:  'No Country Code (365)',
    395:  'No Country Code (395)',
    400:  'No Country Code (400)',
    485:  'No Country Code (485)',
    503:  'No Country Code (503)',
    589:  'No Country Code (589)',
    592:  'No Country Code (592)',
    791:  'No Country Code (791)',
    849:  'No Country Code (849)',
    914:  'No Country Code (914)',
    944:  'No Country Code (944)',
    996:  'No Country Code (996)',
}

#create udf to return the country of origin
country_udf=udf(lambda x: country_codes[x],StringType())


city_codes= {'ALC'	:	'ALCAN             ',
              'ANC'	:	'ANCHORAGE         ',
              'BAR'	:	'BAKER AAF - BAKER ISLAND',
              'DAC'	:	'DALTONS CACHE     ',
              'PIZ'	:	'DEW STATION PT LAY DEW',
              'DTH'	:	'DUTCH HARBOR      ',
              'EGL'	:	'EAGLE             ',
              'FRB'	:	'FAIRBANKS         ',
              'HOM'	:	'HOMER             ',
              'HYD'	:	'HYDER             ',
              'JUN'	:	'JUNEAU            ',
              '5KE'	:	'KETCHIKAN',
              'KET'	:	'KETCHIKAN         ',
              'MOS'	:	'MOSES POINT INTERMEDIATE',
              'NIK'	:	'NIKISKI           ',
              'NOM'	:	'NOM               ',
              'PKC'	:	'POKER CREEK       ',
              'ORI'	:	'PORT LIONS SPB',
              'SKA'	:	'SKAGWAY           ',
              'SNP'	:	'ST. PAUL ISLAND',
              'TKI'	:	'TOKEEN',
              'WRA'	:	'WRANGELL          ',
              'HSV'	:	'HUNTSVILLE',
              'MOB'	:	'MOBILE            ',
              'LIA'	:	'LITTLE ROCK',
              'ROG'	:	'ROGERS ARPT',
              'DOU'	:	'DOUGLAS           ',
              'LUK'	:	'LUKEVILLE         ',
              'MAP'	:	'MARIPOSA            ',
              'NAC'	:	'NACO              ',
              'NOG'	:	'NOGALES           ',
              'PHO'	:	'PHOENIX           ',
              'POR'	:	'PORTAL',
              'SLU'	:	'SAN LUIS          ',
              'SAS'	:	'SASABE            ',
              'TUC'	:	'TUCSON            ',
              'YUI'	:	'YUMA              ',
              'AND'	:	'ANDRADE           ',
              'BUR'	:	'BURBANK',
              'CAL'	:	'CALEXICO          ',
              'CAO'	:	'CAMPO             ',
              'FRE'	:	'FRESNO            ',
              'ICP'	:	'IMPERIAL COUNTY   ',
              'LNB'	:	'LONG BEACH         ',
              'LOS'	:	'LOS ANGELES       ',
              'BFL'	:	'BAKERSFIELD',
              'OAK'	:	'OAKLAND ',
              'ONT'	:	'ONTARIO',
              'OTM'	:	'OTAY MESA          ',
              'BLT'	:	'PACIFIC, HWY. STATION ',
              'PSP'	:	'PALM SPRINGS',
              'SAC'	:	'SACRAMENTO        ',
              'SLS'	:	'SALINAS (BPS)',
              'SDP'	:	'SAN DIEGO',
              'SFR'	:	'SAN FRANCISCO     ',
              'SNJ'	:	'SAN JOSE          ',
              'SLO'	:	'SAN LUIS OBISPO   ',
              'SLI'	:	'SAN LUIS OBISPO (BPS)',
              'SPC'	:	'SAN PEDRO         ',
              'SYS'	:	'SAN YSIDRO        ',
              'SAA'	:	'SANTA ANA         ',
              'STO'	:	'STOCKTON (BPS)',
              'TEC'	:	'TECATE            ',
              'TRV'	:	'TRAVIS-AFB        ',
              'APA'	:	'ARAPAHOE COUNTY',
              'ASE'	:	'ASPEN #ARPT',
              'COS'	:	'COLORADO SPRINGS',
              'DEN'	:	'DENVER            ',
              'DRO'	:	'LA PLATA - DURANGO',
              'BDL'	:	'BRADLEY INTERNATIONAL',
              'BGC'	:	'BRIDGEPORT        ',
              'GRT'	:	'GROTON            ',
              'HAR'	:	'HARTFORD          ',
              'NWH'	:	'NEW HAVEN         ',
              'NWL'	:	'NEW LONDON        ',
              'TST'	:	'NEWINGTON DATA CENTER TEST',
              'WAS'	:	'WASHINGTON DC         ',
              'DOV'	:	'DOVER AFB',
              'DVD'	:	'DOVER-AFB         ',
              'WLL'	:	'WILMINGTON        ',
              'BOC'	:	'BOCAGRANDE        ',
              'SRQ'	:	'BRADENTON - SARASOTA',
              'CAN'	:	'CAPE CANAVERAL    ',
              'DAB'	:	'DAYTONA BEACH INTERNATIONAL',
              'FRN'	:	'FERNANDINA        ',
              'FTL'	:	'FORT LAUDERDALE   ',
              'FMY'	:	'FORT MYERS        ',
              'FPF'	:	'FORT PIERCE       ',
              'HUR'	:	'HURLBURT FIELD',
              'GNV'	:	'J R ALISON MUNI - GAINESVILLE',
              'JAC'	:	'JACKSONVILLE      ',
              'KEY'	:	'KEY WEST          ',
              'LEE'	:	'LEESBURG MUNICIPAL AIRPORT',
              'MLB'	:	'MELBOURNE',
              'MIA'	:	'MIAMI             ',
              'APF'	:	'NAPLES #ARPT',
              'OPF'	:	'OPA LOCKA',
              'ORL'	:	'ORLANDO           ',
              'PAN'	:	'PANAMA CITY       ',
              'PEN'	:	'PENSACOLA         ',
              'PCF'	:	'PORT CANAVERAL    ',
              'PEV'	:	'PORT EVERGLADES   ',
              'PSJ'	:	'PORT ST JOE       ',
              'SFB'	:	'SANFORD           ',
              'SGJ'	:	'ST AUGUSTINE ARPT',
              'SAU'	:	'ST AUGUSTINE      ',
              'FPR'	:	'ST LUCIE COUNTY',
              'SPE'	:	'ST PETERSBURG     ',
              'TAM'	:	'TAMPA             ',
              'WPB'	:	'WEST PALM BEACH   ',
              'ATL'	:	'ATLANTA           ',
              'BRU'	:	'BRUNSWICK         ',
              'AGS'	:	'BUSH FIELD - AUGUSTA',
              'SAV'	:	'SAVANNAH          ',
              'AGA'	:	'AGANA             ',
              'HHW'	:	'HONOLULU          ',
              'OGG'	:	'KAHULUI - MAUI',
              'KOA'	:	'KEAHOLE-KONA      ',
              'LIH'	:	'LIHUE             ',
              'CID'	:	'CEDAR RAPIDS/IOWA CITY',
              'DSM'	:	'DES MOINES',
              'BOI'	:	'AIR TERM. (GOWEN FLD) BOISE',
              'EPI'	:	'EASTPORT          ',
              'IDA'	:	'FANNING FIELD - IDAHO FALLS',
              'PTL'	:	'PORTHILL          ',
              'SPI'	:	'CAPITAL - SPRINGFIELD',
              'CHI'	:	'CHICAGO           ',
              'DPA'	:	'DUPAGE COUNTY',
              'PIA'	:	'GREATER PEORIA',
              'RFD'	:	'GREATER ROCKFORD',
              'UGN'	:	'MEMORIAL - WAUKEGAN',
              'GAR'	:	'GARY              ',
              'HMM'	:	'HAMMOND           ',
              'INP'	:	'INDIANAPOLIS      ',
              'MRL'	:	'MERRILLVILLE      ',
              'SBN'	:	'SOUTH BEND',
              'ICT'	:	'MID-CONTINENT - WITCHITA',
              'LEX'	:	'BLUE GRASS - LEXINGTON',
              'LOU'	:	'LOUISVILLE        ',
              'BTN'	:	'BATON ROUGE       ',
              'LKC'	:	'LAKE CHARLES      ',
              'LAK'	:	'LAKE CHARLES (BPS)',
              'MLU'	:	'MONROE',
              'MGC'	:	'MORGAN CITY       ',
              'NOL'	:	'NEW ORLEANS       ',
              'BOS'	:	'BOSTON            ',
              'GLO'	:	'GLOUCESTER        ',
              'BED'	:	'HANSCOM FIELD - BEDFORD',
              'LYN'	:	'LYNDEN            ',
              'ADW'	:	'ANDREWS AFB',
              'BAL'	:	'BALTIMORE         ',
              'MKG'	:	'MUSKEGON',
              'PAX'	:	'PATUXENT RIVER    ',
              'BGM'	:	'BANGOR            ',
              'BOO'	:	'BOOTHBAY HARBOR   ',
              'BWM'	:	'BRIDGEWATER       ',
              'BCK'	:	'BUCKPORT          ',
              'CLS'	:	'CALAIS   ',
              'CRB'	:	'CARIBOU           ',
              'COB'	:	'COBURN GORE       ',
              'EST'	:	'EASTCOURT         ',
              'EPT'	:	'EASTPORT MUNICIPAL',
              'EPM'	:	'EASTPORT          ',
              'FOR'	:	'FOREST CITY       ',
              'FTF'	:	'FORT FAIRFIELD    ',
              'FTK'	:	'FORT KENT         ',
              'HML'	:	'HAMIIN            ',
              'HTM'	:	'HOULTON           ',
              'JKM'	:	'JACKMAN           ',
              'KAL'	:	'KALISPEL          ',
              'LIM'	:	'LIMESTONE         ',
              'LUB'	:	'LUBEC             ',
              'MAD'	:	'MADAWASKA         ',
              'POM'	:	'PORTLAND          ',
              'RGM'	:	'RANGELEY (BPS)',
              'SBR'	:	'SOUTH BREWER      ',
              'SRL'	:	'ST AURELIE        ',
              'SPA'	:	'ST PAMPILE        ',
              'VNB'	:	'VAN BUREN         ',
              'VCB'	:	'VANCEBORO         ',
              'AGN'	:	'ALGONAC           ',
              'ALP'	:	'ALPENA            ',
              'BCY'	:	'BAY CITY          ',
              'DET'	:	'DETROIT           ',
              'GRP'	:	'GRAND RAPIDS',
              'GRO'	:	'GROSSE ISLE       ',
              'ISL'	:	'ISLE ROYALE       ',
              'MRC'	:	'MARINE CITY       ',
              'MRY'	:	'MARYSVILLE        ',
              'PTK'	:	'OAKLAND COUNTY - PONTIAC',
              'PHU'	:	'PORT HURON        ',
              'RBT'	:	'ROBERTS LANDING   ',
              'SAG'	:	'SAGINAW           ',
              'SSM'	:	'SAULT STE. MARIE  ',
              'SCL'	:	'ST CLAIR          ',
              'YIP'	:	'WILLOW RUN - YPSILANTI',
              'BAU'	:	'BAUDETTE          ',
              'CAR'	:	'CARIBOU MUNICIPAL AIRPORT',
              'GTF'	:	'Collapsed into INT',
              'INL'	:	'Collapsed into INT',
              'CRA'	:	'CRANE LAKE        ',
              'MIC'	:	'CRYSTAL MUNICIPAL AIRPORT',
              'DUL'	:	'DULUTH            ',
              'ELY'	:	'ELY               ',
              'GPM'	:	'GRAND PORTAGE     ',
              'SVC'	:	'GRANT COUNTY - SILVER CITY',
              'INT'	:	'INT''L FALLS      ',
              'LAN'	:	'LANCASTER         ',
              'MSP'	:	'MINN./ST PAUL     ',
              'LIN'	:	'NORTHERN SVC CENTER   ',
              'NOY'	:	'NOYES             ',
              'PIN'	:	'PINE CREEK        ',
              '48Y'	:	'PINECREEK BORDER ARPT',
              'RAN'	:	'RAINER            ',
              'RST'	:	'ROCHESTER',
              'ROS'	:	'ROSEAU            ',
              'SPM'	:	'ST PAUL           ',
              'WSB'	:	'WARROAD INTL',
              'WAR'	:	'WARROAD           ',
              'KAN'	:	'KANSAS CITY       ',
              'SGF'	:	'SPRINGFIELD-BRANSON',
              'STL'	:	'ST LOUIS          ',
              'WHI'	:	'WHITETAIL         ',
              'WHM'	:	'WILD HORSE        ',
              'GPT'	:	'BILOXI REGIONAL',
              'GTR'	:	'GOLDEN TRIANGLE LOWNDES CNTY',
              'GUL'	:	'GULFPORT          ',
              'PAS'	:	'PASCAGOULA        ',
              'JAN'	:	'THOMPSON FIELD - JACKSON',
              'BIL'	:	'BILLINGS          ',
              'BTM'	:	'BUTTE             ',
              'CHF'	:	'CHIEF MT          ',
              'CTB'	:	'CUT BANK MUNICIPAL',
              'CUT'	:	'CUT BANK          ',
              'DLB'	:	'DEL BONITA        ',
              'EUR'	:	'EUREKA (BPS)',
              'BZN'	:	'GALLATIN FIELD - BOZEMAN',
              'FCA'	:	'GLACIER NATIONAL PARK',
              'GGW'	:	'GLASGOW           ',
              'GRE'	:	'GREAT FALLS       ',
              'HVR'	:	'HAVRE             ',
              'HEL'	:	'HELENA            ',
              'LWT'	:	'LEWISTON          ',
              'MGM'	:	'MORGAN            ',
              'OPH'	:	'OPHEIM            ',
              'PIE'	:	'PIEGAN            ',
              'RAY'	:	'RAYMOND           ',
              'ROO'	:	'ROOSVILLE         ',
              'SCO'	:	'SCOBEY            ',
              'SWE'	:	'SWEETGTASS        ',
              'TRL'	:	'TRIAL CREEK       ',
              'TUR'	:	'TURNER            ',
              'WCM'	:	'WILLOW CREEK      ',
              'CLT'	:	'CHARLOTTE         ',
              'FAY'	:	'FAYETTEVILLE',
              'MRH'	:	'MOREHEAD CITY     ',
              'FOP'	:	'MORRIS FIELDS AAF',
              'GSO'	:	'PIEDMONT TRIAD INTL AIRPORT',
              'RDU'	:	'RALEIGH/DURHAM    ',
              'SSC'	:	'SHAW AFB - SUMTER',
              'WIL'	:	'WILMINGTON        ',
              'AMB'	:	'AMBROSE           ',
              'ANT'	:	'ANTLER            ',
              'CRY'	:	'CARBURY           ',
              'DNS'	:	'DUNSEITH          ',
              'FAR'	:	'FARGO             ',
              'FRT'	:	'FORTUNA           ',
              'GRF'	:	'GRAND FORKS       ',
              'HNN'	:	'HANNAH            ',
              'HNS'	:	'HANSBORO          ',
              'MAI'	:	'MAIDA             ',
              'MND'	:	'MINOT             ',
              'NEC'	:	'NECHE             ',
              'NOO'	:	'NOONAN            ',
              'NRG'	:	'NORTHGATE         ',
              'PEM'	:	'PEMBINA           ',
              'SAR'	:	'SARLES            ',
              'SHR'	:	'SHERWOOD          ',
              'SJO'	:	'ST JOHN           ',
              'WAL'	:	'WALHALLA          ',
              'WHO'	:	'WESTHOPE          ',
              'WND'	:	'WILLISTON         ',
              'OMA'	:	'OMAHA             ',
              'LEB'	:	'LEBANON           ',
              'MHT'	:	'MANCHESTER',
              'PNH'	:	'PITTSBURG         ',
              'PSM'	:	'PORTSMOUTH        ',
              'BYO'	:	'BAYONNE           ',
              'CNJ'	:	'CAMDEN            ',
              'HOB'	:	'HOBOKEN           ',
              'JER'	:	'JERSEY CITY       ',
              'WRI'	:	'MC GUIRE AFB - WRIGHTSOWN',
              'MMU'	:	'MORRISTOWN',
              'NEW'	:	'NEWARK/TETERBORO  ',
              'PER'	:	'PERTH AMBOY       ',
              'ACY'	:	'POMONA FIELD - ATLANTIC CITY',
              'ALA'	:	'ALAMAGORDO (BPS)',
              'ABQ'	:	'ALBUQUERQUE       ',
              'ANP'	:	'ANTELOPE WELLS    ',
              'CRL'	:	'CARLSBAD          ',
              'COL'	:	'COLUMBUS          ',
              'CDD'	:	'CRANE LAKE - ST. LOUIS CNTY',
              'DNM'	:	'DEMING (BPS)',
              'LAS'	:	'LAS CRUCES        ',
              'LOB'	:	'LORDSBURG (BPS)',
              'RUI'	:	'RUIDOSO',
              'STR'	:	'SANTA TERESA      ',
              'RNO'	:	'CANNON INTL - RENO/TAHOE',
              'FLX'	:	'FALLON MUNICIPAL AIRPORT',
              'LVG'	:	'LAS VEGAS         ',
              'REN'	:	'RENO              ',
              'ALB'	:	'ALBANY            ',
              'AXB'	:	'ALEXANDRIA BAY    ',
              'BUF'	:	'BUFFALO           ',
              'CNH'	:	'CANNON CORNERS',
              'CAP'	:	'CAPE VINCENT      ',
              'CHM'	:	'CHAMPLAIN         ',
              'CHT'	:	'CHATEAUGAY        ',
              'CLA'	:	'CLAYTON           ',
              'FTC'	:	'FORT COVINGTON    ',
              'LAG'	:	'LA GUARDIA        ',
              'LEW'	:	'LEWISTON          ',
              'MAS'	:	'MASSENA           ',
              'MAG'	:	'MCGUIRE AFB       ',
              'MOO'	:	'MOORES            ',
              'MRR'	:	'MORRISTOWN        ',
              'NYC'	:	'NEW YORK          ',
              'NIA'	:	'NIAGARA FALLS     ',
              'OGD'	:	'OGDENSBURG        ',
              'OSW'	:	'OSWEGO            ',
              'ELM'	:	'REGIONAL ARPT - HORSEHEAD',
              'ROC'	:	'ROCHESTER         ',
              'ROU'	:	'ROUSES POINT      ',
              'SWF'	:	'STEWART - ORANGE CNTY',
              'SYR'	:	'SYRACUSE          ',
              'THO'	:	'THOUSAND ISLAND BRIDGE',
              'TRO'	:	'TROUT RIVER       ',
              'WAT'	:	'WATERTOWN         ',
              'HPN'	:	'WESTCHESTER - WHITE PLAINS',
              'WRB'	:	'WHIRLPOOL BRIDGE',
              'YOU'	:	'YOUNGSTOWN        ',
              'AKR'	:	'AKRON             ',
              'ATB'	:	'ASHTABULA         ',
              'CIN'	:	'CINCINNATI        ',
              'CLE'	:	'CLEVELAND         ',
              'CLM'	:	'COLUMBUS          ',
              'LOR'	:	'LORAIN            ',
              'MBO'	:	'MARBLE HEADS      ',
              'SDY'	:	'SANDUSKY          ',
              'TOL'	:	'TOLEDO            ',
              'OKC'	:	'OKLAHOMA CITY     ',
              'TUL'	:	'TULSA',
              'AST'	:	'ASTORIA           ',
              'COO'	:	'COOS BAY          ',
              'HIO'	:	'HILLSBORO',
              'MED'	:	'MEDFORD           ',
              'NPT'	:	'NEWPORT           ',
              'POO'	:	'PORTLAND          ',
              'PUT'	:	'PUT-IN-BAY        ',
              'RDM'	:	'ROBERTS FIELDS - REDMOND',
              'ERI'	:	'ERIE              ',
              'MDT'	:	'HARRISBURG',
              'HSB'	:	'HARRISONBURG      ',
              'PHI'	:	'PHILADELPHIA      ',
              'PIT'	:	'PITTSBURG         ',
              'AGU'	:	'AGUADILLA         ',
              'BQN'	:	'BORINQUEN - AGUADILLO',
              'JCP'	:	'CULEBRA - BENJAMIN RIVERA',
              'ENS'	:	'ENSENADA          ',
              'FAJ'	:	'FAJARDO           ',
              'HUM'	:	'HUMACAO           ',
              'JOB'	:	'JOBOS             ',
              'MAY'	:	'MAYAGUEZ          ',
              'PON'	:	'PONCE             ',
              'PSE'	:	'PONCE-MERCEDITA',
              'SAJ'	:	'SAN JUAN          ',
              'VQS'	:	'VIEQUES-ARPT',
              'PRO'	:	'PROVIDENCE        ',
              'PVD'	:	'THEODORE FRANCIS - WARWICK',
              'CHL'	:	'CHARLESTON        ',
              'CAE'	:	'COLUMBIA #ARPT',
              'GEO'	:	'GEORGETOWN        ',
              'GSP'	:	'GREENVILLE',
              'GRR'	:	'GREER',
              'MYR'	:	'MYRTLE BEACH',
              'SPF'	:	'BLACK HILLS, SPEARFISH',
              'HON'	:	'HOWES REGIONAL ARPT - HURON',
              'SAI'	:	'SAIPAN, SPN           ',
              'TYS'	:	'MC GHEE TYSON - ALCOA',
              'MEM'	:	'MEMPHIS           ',
              'NSV'	:	'NASHVILLE         ',
              'TRI'	:	'TRI CITY ARPT',
              'ADS'	:	'ADDISON AIRPORT- ADDISON',
              'ADT'	:	'AMISTAD DAM       ',
              'ANZ'	:	'ANZALDUAS',
              'AUS'	:	'AUSTIN            ',
              'BEA'	:	'BEAUMONT          ',
              'BBP'	:	'BIG BEND PARK (BPS)',
              'SCC'	:	'BP SPEC COORD. CTR',
              'BTC'	:	'BP TACTICAL UNIT  ',
              'BOA'	:	'BRIDGE OF AMERICAS',
              'BRO'	:	'BROWNSVILLE       ',
              'CRP'	:	'CORPUS CHRISTI    ',
              'DAL'	:	'DALLAS            ',
              'DLR'	:	'DEL RIO           ',
              'DNA'	:	'DONNA',
              'EGP'	:	'EAGLE PASS        ',
              'ELP'	:	'EL PASO           ',
              'FAB'	:	'FABENS            ',
              'FAL'	:	'FALCON HEIGHTS    ',
              'FTH'	:	'FORT HANCOCK      ',
              'AFW'	:	'FORT WORTH ALLIANCE',
              'FPT'	:	'FREEPORT          ',
              'GAL'	:	'GALVESTON         ',
              'HLG'	:	'HARLINGEN         ',
              'HID'	:	'HIDALGO           ',
              'HOU'	:	'HOUSTON           ',
              'SGR'	:	'HULL FIELD, SUGAR LAND ARPT',
              'LLB'	:	'JUAREZ-LINCOLN BRIDGE',
              'LCB'	:	'LAREDO COLUMBIA BRIDGE',
              'LRN'	:	'LAREDO NORTH      ',
              'LAR'	:	'LAREDO            ',
              'LSE'	:	'LOS EBANOS        ',
              'IND'	:	'LOS INDIOS',
              'LOI'	:	'LOS INDIOS        ',
              'MRS'	:	'MARFA (BPS)',
              'MCA'	:	'MCALLEN           ',
              'MAF'	:	'ODESSA REGIONAL',
              'PDN'	:	'PASO DEL NORTE,TX     ',
              'PBB'	:	'PEACE BRIDGE      ',
              'PHR'	:	'PHARR             ',
              'PAR'	:	'PORT ARTHUR       ',
              'ISB'	:	'PORT ISABEL       ',
              'POE'	:	'PORT OF EL PASO   ',
              'PRE'	:	'PRESIDIO          ',
              'PGR'	:	'PROGRESO          ',
              'RIO'	:	'RIO GRANDE CITY   ',
              'ROM'	:	'ROMA              ',
              'SNA'	:	'SAN ANTONIO       ',
              'SNN'	:	'SANDERSON         ',
              'VIB'	:	'VETERAN INTL BRIDGE',
              'YSL'	:	'YSLETA            ',
              'CHA'	:	'CHARLOTTE AMALIE  ',
              'CHR'	:	'CHRISTIANSTED     ',
              'CRU'	:	'CRUZ BAY, ST JOHN ',
              'FRK'	:	'FREDERIKSTED      ',
              'STT'	:	'ST THOMAS         ',
              'LGU'	:	'CACHE AIRPORT - LOGAN',
              'SLC'	:	'SALT LAKE CITY    ',
              'CHO'	:	'ALBEMARLE CHARLOTTESVILLE',
              'DAA'	:	'DAVISON AAF - FAIRFAX CNTY',
              'HOP'	:	'HOPEWELL          ',
              'HEF'	:	'MANASSAS #ARPT',
              'NWN'	:	'NEWPORT           ',
              'NOR'	:	'NORFOLK           ',
              'RCM'	:	'RICHMOND          ',
              'ABS'	:	'ALBURG SPRINGS    ',
              'ABG'	:	'ALBURG            ',
              'BEB'	:	'BEEBE PLAIN       ',
              'BEE'	:	'BEECHER FALLS     ',
              'BRG'	:	'BURLINGTON        ',
              'CNA'	:	'CANAAN            ',
              'DER'	:	'DERBY LINE (I-91) ',
              'DLV'	:	'DERBY LINE (RT. 5)',
              'ERC'	:	'EAST RICHFORD     ',
              'HIG'	:	'HIGHGATE SPRINGS  ',
              'MOR'	:	'MORSES LINE       ',
              'NPV'	:	'NEWPORT           ',
              'NRT'	:	'NORTH TROY        ',
              'NRN'	:	'NORTON            ',
              'PIV'	:	'PINNACLE ROAD     ',
              'RIF'	:	'RICHFORT          ',
              'STA'	:	'ST ALBANS         ',
              'SWB'	:	'SWANTON (BP - SECTOR HQ)',
              'WBE'	:	'WEST BERKSHIRE    ',
              'ABE'	:	'ABERDEEN          ',
              'ANA'	:	'ANACORTES         ',
              'BEL'	:	'BELLINGHAM        ',
              'BLI'	:	'BELLINGHAMSHINGTON #INTL',
              'BLA'	:	'BLAINE            ',
              'BWA'	:	'BOUNDARY          ',
              'CUR'	:	'CURLEW (BPS)',
              'DVL'	:	'DANVILLE          ',
              'EVE'	:	'EVERETT           ',
              'FER'	:	'FERRY             ',
              'FRI'	:	'FRIDAY HARBOR     ',
              'FWA'	:	'FRONTIER          ',
              'KLM'	:	'KALAMA            ',
              'LAU'	:	'LAURIER           ',
              'LON'	:	'LONGVIEW          ',
              'MET'	:	'METALINE FALLS    ',
              'MWH'	:	'MOSES LAKE GRANT COUNTY ARPT',
              'NEA'	:	'NEAH BAY          ',
              'NIG'	:	'NIGHTHAWK         ',
              'OLY'	:	'OLYMPIA           ',
              'ORO'	:	'OROVILLE          ',
              'PWB'	:	'PASCO             ',
              'PIR'	:	'POINT ROBERTS     ',
              'PNG'	:	'PORT ANGELES      ',
              'PTO'	:	'PORT TOWNSEND     ',
              'SEA'	:	'SEATTLE           ',
              'SPO'	:	'SPOKANE           ',
              'SUM'	:	'SUMAS             ',
              'TAC'	:	'TACOMA            ',
              'PSC'	:	'TRI-CITIES - PASCO',
              'VAN'	:	'VANCOUVER         ',
              'AGM'	:	'ALGOMA            ',
              'BAY'	:	'BAYFIELD          ',
              'GRB'	:	'GREEN BAY         ',
              'MNW'	:	'MANITOWOC         ',
              'MIL'	:	'MILWAUKEE         ',
              'MSN'	:	'TRUAX FIELD - DANE COUNTY',
              'CHS'	:	'CHARLESTON        ',
              'CLK'	:	'CLARKSBURG        ',
              'BLF'	:	'MERCER COUNTY',
              'CSP'	:	'CASPER            ',
              'XXX': 'NOT REPORTED/UNKNOWN  ',
              '888': 'UNIDENTIFED AIR / SEAPORT',
              'UNK': 'UNKNOWN POE           ',
              'CLG': 'CALGARY, CANADA       ',
              'EDA': 'EDMONTON, CANADA      ',
              'YHC': 'HAKAI PASS, CANADA',
              'HAL': 'Halifax, NS, Canada   ',
              'MON': 'MONTREAL, CANADA      ',
              'OTT': 'OTTAWA, CANADA        ',
              'YXE': 'SASKATOON, CANADA',
              'TOR': 'TORONTO, CANADA       ',
              'VCV': 'VANCOUVER, CANADA     ',
              'VIC': 'VICTORIA, CANADA      ',
              'WIN': 'WINNIPEG, CANADA      ',
              'AMS': 'AMSTERDAM-SCHIPHOL, NETHERLANDS',
              'ARB': 'ARUBA, NETH ANTILLES  ',
              'BAN': 'BANKOK, THAILAND      ',
              'BEI': 'BEICA #ARPT, ETHIOPIA',
              'PEK': 'BEIJING CAPITAL INTL, PRC',
              'BDA': 'KINDLEY FIELD, BERMUDA',
              'BOG': 'BOGOTA, EL DORADO #ARPT, COLOMBIA',
              'EZE': 'BUENOS AIRES, MINISTRO PIST, ARGENTINA',
              'CUN': 'CANCUN, MEXICO',
              'CRQ': 'CARAVELAS, BA #ARPT, BRAZIL',
              'MVD': 'CARRASCO, URUGUAY',
              'DUB': 'DUBLIN, IRELAND       ',
              'FOU': 'FOUGAMOU #ARPT, GABON',
              'FBA': 'FREEPORT, BAHAMAS      ',
              'MTY': 'GEN M. ESCOBEDO, Monterrey, MX',
              'HMO': 'GEN PESQUEIRA GARCIA, MX',
              'GCM': 'GRAND CAYMAN, CAYMAN ISLAND',
              'GDL': 'GUADALAJARA, MIGUEL HIDAL, MX',
              'HAM': 'HAMILTON, BERMUDA     ',
              'ICN': 'INCHON, SEOUL KOREA',
              'IWA': 'INVALID - IWAKUNI, JAPAN',
              'CND': 'KOGALNICEANU, ROMANIA',
              'LAH': 'LABUHA ARPT, INDONESIA',
              'DUR': 'LOUIS BOTHA, SOUTH AFRICA',
              'MAL': 'MANGOLE ARPT, INDONESIA',
              'MDE': 'MEDELLIN, COLOMBIA',
              'MEX': 'JUAREZ INTL, MEXICO CITY, MX',
              'LHR': 'MIDDLESEX, ENGLAND',
              'NBO': 'NAIROBI, KENYA        ',
              'NAS': 'NASSAU, BAHAMAS       ',
              'NCA': 'NORTH CAICOS, TURK & CAIMAN',
              'PTY': 'OMAR TORRIJOS, PANAMA',
              'SPV': 'PAPUA, NEW GUINEA',
              'UIO': 'QUITO (MARISCAL SUCR), ECUADOR',
              'RIT': 'ROME, ITALY           ',
              'SNO': 'SAKON NAKHON #ARPT, THAILAND',
              'SLP': 'SAN LUIS POTOSI #ARPT, MEXICO',
              'SAN': 'SAN SALVADOR, EL SALVADOR',
              'SRO': 'SANTANA RAMOS #ARPT, COLOMBIA',
              'GRU': 'GUARULHOS INTL, SAO PAULO, BRAZIL',
              'SHA': 'SHANNON, IRELAND      ',
              'HIL': 'SHILLAVO, ETHIOPIA',
              'TOK': 'TOROKINA #ARPT, PAPUA, NEW GUINEA',
              'VER': 'VERACRUZ, MEXICO',
              'LGW': 'WEST SUSSEX, ENGLAND  ',
              'ZZZ': 'MEXICO Land (Banco de Mexico) ',
              'CHN': 'No PORT Code (CHN)',
              'CNC': 'CANNON CORNERS, NY',
              'MAA': 'Abu Dhabi',
              'AG0': 'MAGNOLIA',
              'BHM': 'BAR HARBOR',
              'BHX': 'BIRMINGHAM',
              'CAK': 'AKRON',
              'FOK': 'SUFFOLK COUNTY',
              'LND': 'LANDER',
              'MAR': 'MARFA',
              'MLI': 'MOLINE',
              'RIV': 'RIVERSIDE',
              'RME': 'ROME',
              'VNY': 'VAN NUYS',
              'YUM': 'YUMA',
             'W55': 'SEATTLE'
              }

city_code_udf=udf(lambda x:city_codes[x],StringType())