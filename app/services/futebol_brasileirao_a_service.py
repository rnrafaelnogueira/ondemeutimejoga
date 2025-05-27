import pdfplumber
import duckdb
from typing import List, Dict
from fuzzywuzzy import fuzz
from datetime import datetime  # Adicionado para manipulação de datas

class FutebolBrasileiraoAService:

    def __init__(self, db_path):
        self.db_path = db_path

    def transformar_data(self, data_str):
        dias_semana = {
            'dom': 'Domingo',
            'seg': 'Segunda',
            'ter': 'Terça',
            'qua': 'Quarta',
            'qui': 'Quinta',
            'sex': 'Sexta',
            'sáb': 'Sábado'
        }
        
        if not data_str:
            return None
            
        data, dia_abreviado = data_str.split()
        dia_completo = dias_semana.get(dia_abreviado, dia_abreviado)
        return f"{data} - {dia_completo}"

    def extract_data_from_pdf(self, pdf_path) -> List[Dict[str, str]]:
        """Extrai os dados do PDF e retorna uma lista de dicionários."""
        data = []
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages[2:]:
                    table = page.extract_table()
                    
                    if table:
                        table = table[1:]
                        
                        for row in table:
                            if len(row) >= 14:
                                record = {
                                    "Data": row[2] if row[2] else None,
                                    "Hora": row[4] if row[4] else None,
                                    "Jogo": row[5] if row[5] else None,
                                    "Estadio": row[6] if row[6] else None,
                                    "Cidade": row[7] if row[7] else None,
                                    "UF": row[8] if row[8] else None,
                                    "TV_1": row[9] if row[9] else None,
                                    "TV_2": row[10] if row[10] else None,
                                    "TV_3": row[11] if row[11] else None,
                                    "TV_4": row[12] if row[12] else None,
                                    "TV_5": row[13] if row[13] else None,
                                    "TV_6": row[14] if row[14] else None,
                                }
                                data.append(record)
            return data
        except Exception as e:
            raise Exception(f"Erro ao extrair dados do PDF: {str(e)}")

    def save_to_duckdb(self, table_name, data: List[Dict[str, str]]):
        """Salva os dados extraídos no banco de dados DuckDB."""
        try:
            conn = duckdb.connect(self.db_path)

            conn.execute(f"DROP TABLE IF EXISTS {table_name}")

            conn.execute(f"""
                CREATE TABLE {table_name} (
                    Data TEXT,
                    Hora TEXT,
                    Jogo TEXT,
                    Estadio TEXT,
                    Cidade TEXT,
                    UF TEXT,
                    TV_1 TEXT,
                    TV_2 TEXT,
                    TV_3 TEXT,
                    TV_4 TEXT,
                    TV_5 TEXT,
                    TV_6 TEXT
                )
            """)
            
            for record in data:
                conn.execute(f"""
                    INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    record["Data"], record["Hora"], record["Jogo"], record["Estadio"], 
                    record["Cidade"], record["UF"], record["TV_1"], record["TV_2"], 
                    record["TV_3"], record["TV_4"], record["TV_5"], record["TV_6"]
                ))
            conn.close()
        except Exception as e:
            raise Exception(f"Erro ao salvar dados no DuckDB: {str(e)}")

    def parse_date(self, date_str):
        """Converte a string de data no formato 'DD/MM - dia' para um objeto datetime."""
        if not date_str:
            return None

        try:
            date_part = date_str.split(" - ")[0]  # Pega apenas a parte "DD/MM"
            day, month = date_part.split('/')
            current_year = datetime.now().year
            return datetime.strptime(f"{day}/{month}/{current_year}", "%d/%m/%Y")
        except:
            return None

    def get_all_texts(self, table_name, team_name: str = None, similarity_threshold: int = 80) -> List[Dict[str, str]]:
        """
        Retorna todos os registros da tabela como uma lista de dicionários.
        Se `team_name` for fornecido, filtra os registros com base na similaridade dos nomes das equipes.
        Retorna apenas jogos a partir da data atual.
        """
        try:
            conn = duckdb.connect(self.db_path)
            result = conn.execute(f"SELECT * FROM {table_name} WHERE (Hora <> 'HORA') LIMIT 42").fetchall()
            conn.close()

            columns = [
                "Data", "Hora", "Jogo", "Estadio", "Cidade", "UF", 
                "TV_1", "TV_2", "TV_3", "TV_4", "TV_5", "TV_6"
            ]
            data = [dict(zip(columns, row)) for row in result]

            tv_mapping = {
                "1": "Globo",
                "2": "Record",
                "3": "Sportv",
                "4": "Amazon",
                "5": "Youtube",
                "6": "Premiere"
            }

            today = datetime.now().date()
            filtered_data = []
            last_valid_date = None

            for record in data:
                # Processa a data
                if record["Data"]:
                    raw_date = record["Data"].split("\n")[0]
                    last_valid_date = self.transformar_data(raw_date)
                    record["Data"] = last_valid_date
                else:
                    record["Data"] = last_valid_date

                # Verifica se a data é válida e é hoje ou no futuro
                parsed_date = self.parse_date(record["Data"])
                if not parsed_date or parsed_date.date() < today:
                    continue

                # Mapeia os canais de TV
                for tv_key in ["TV_1", "TV_2", "TV_3", "TV_4", "TV_5", "TV_6"]:
                    if record[tv_key] in tv_mapping:
                        record[tv_key] = tv_mapping[record[tv_key]]

                # Filtra por similaridade do nome da equipe
                if team_name:
                    jogo = record.get("Jogo", "")
                    if jogo:
                        similarity = fuzz.partial_ratio(team_name.lower(), jogo.lower())
                        if similarity >= similarity_threshold:
                            filtered_data.append(record)
                else:
                    filtered_data.append(record)

            return filtered_data
        except Exception as e:
            raise Exception(f"Erro ao recuperar dados do DuckDB: {str(e)}")