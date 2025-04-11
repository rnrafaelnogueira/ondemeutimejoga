import pdfplumber
import duckdb
from typing import List, Dict
from fuzzywuzzy import fuzz
from datetime import datetime, timedelta

class FutebolLibertadoresService:

    def __init__(self, db_path):
        self.db_path = db_path

    # Função para transformar a string
    def transformar_data(self, data_str):
        dias_semana = {
            'DOM': 'Domingo',
            'SEG': 'Segunda',
            'TER': 'Terça',
            'QUA': 'Quarta',
            'QUI': 'Quinta',
            'SEX': 'Sexta',
            'SAB': 'Sábado'
        }
        
        # Obtém o nome completo do dia da semana
        dia_completo = dias_semana.get(data_str)  # Usa a abreviação se não encontrar no dicionário
        
        # Retorna a string formatada
        return dia_completo

    def extract_data_from_pdf(self, pdf_path) -> List[Dict[str, str]]:
        """Extrai os dados do PDF e retorna uma lista de dicionários."""
        data = []
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    # Extrai a tabela da página
                    table = page.extract_table()
                    
                    if table:
                        # Remove a primeira linha (cabeçalho)
                        table = table[1:]
                        
                        # Processa cada linha da tabela
                        for row in table:
                            if len(row) >= 16:  # Verifica se a linha tem colunas suficientes
                                record = {
                                    "id": row[0] if row[0] else None,
                                    "Dia": row[1] if row[1] else None,  # MAR/TER
                                    "Fecha": row[2] if row[2] else None,  # 01/04
                                    "Ciudad": row[3] if row[3] else None,  # Asunción
                                    "Pais": row[4] if row[4] else None,  # PAR
                                    "Estadio": row[5] if row[5] else None,  # UENO La Nueva Olla
                                    "Hora_Local": row[6] if row[6] else None,  # 19:00
                                    "GMT": row[7] if row[7] else None,  # 22:00
                                    "Hora_BRA": row[8] if row[8] else None,  # 19:00
                                    "Equipo_A": row[9] if row[9] else None,  # Cerro Porteño (PAR)
                                    "Versos": row[10] if row[10] else None,  # vs
                                    "Equipo_B": row[11] if row[11] else None,  # Bolívar (BOL)
                                    "Grupo": row[12] if row[12] else None,  # Grupo G
                                    "Cable_1": row[13] if row[13] else None,  # Paramount
                                    "Abierta_1": row[14] if row[14] else None,  # ESPN / Disney+
                                    "Cable_2": row[15] if row[15] else None,  # Outro valor de Cable
                                    "Abierta_2": row[16] if row[16] else None  # Outro valor de Abierta
                                }
                                data.append(record)
            return data
        except Exception as e:
            raise Exception(f"Erro ao extrair dados do PDF: {str(e)}")

    def save_to_duckdb(self, table_name, data: List[Dict[str, str]]):
        """Salva os dados extraídos no banco de dados DuckDB."""
        try:
            conn = duckdb.connect(self.db_path)
            conn.execute(f"CREATE SEQUENCE IF NOT EXISTS {table_name}_seq")
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id TEXT,
                    Dia TEXT,
                    Fecha TEXT,
                    Ciudad TEXT,
                    Pais TEXT,
                    Estadio TEXT,
                    Hora_Local TEXT,
                    GMT TEXT,
                    Hora_BRA TEXT,
                    Equipo_A TEXT,
                    Versos TEXT,
                    Equipo_B TEXT,
                    Grupo TEXT,
                    Cable_1 TEXT,
                    Abierta_1 TEXT,
                    Cable_2 TEXT,
                    Abierta_2 TEXT
                )
            """)
            # Insere cada registro na tabela
            for record in data:
                conn.execute(f"""
                    INSERT INTO {table_name} (
                        id, Dia, Fecha, Ciudad, Pais, Estadio, Hora_Local, GMT, Hora_BRA, Equipo_A, Versos, Equipo_B, Grupo, Cable_1, Abierta_1, Cable_2, Abierta_2
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    record["id"], record["Dia"], record["Fecha"], record["Ciudad"], record["Pais"], record["Estadio"],
                    record["Hora_Local"], record["GMT"], record["Hora_BRA"], record["Equipo_A"], record["Versos"], record["Equipo_B"],
                    record["Grupo"], record["Cable_1"], record["Abierta_1"], record["Cable_2"], record["Abierta_2"]
                ))
            conn.close()
        except Exception as e:
            raise Exception(f"Erro ao salvar dados no DuckDB: {str(e)}")

    def get_all_texts(self, table_name, team_name: str = None, similarity_threshold: int = 60) -> List[Dict[str, str]]:
        """
        Retorna todos os registros da tabela como uma lista de dicionários.
        Se `team_name` for fornecido, filtra os registros com base na similaridade dos nomes das equipes.
        Retorna apenas os registros dos próximos 15 dias com base no campo `Fecha`.
        """
        try:
            conn = duckdb.connect(self.db_path)
            result = conn.execute(f"SELECT * FROM {table_name} WHERE id <> '#' and id not like '%Fase%' and Equipo_B is not null ").fetchall()
            conn.close()

            # Converte os registros em dicionários
            columns = [
                "id", "Dia", "Fecha", "Ciudad", "Pais", "Estadio", "Hora_Local", "GMT", "Hora_BRA",
                "Equipo_A", "Versos", "Equipo_B", "Grupo", "Cable_1", "Abierta_1", "Cable_2", "Abierta_2"
            ]
            data = [dict(zip(columns, row)) for row in result]

            # Filtra os dados com base na similaridade do nome da equipe, se fornecido
            filtered_data = []
            today = datetime.now()
            fifteen_days_later = today + timedelta(days=15)

            for record in data:
                if record["Equipo_A"] and record["Equipo_B"]:
                    # Completa o campo Fecha com o ano atual
                    fecha_str = record["Fecha"] + f"/{today.year}"
                    fecha = datetime.strptime(fecha_str, "%d/%m/%Y")

                    if today.date() <= fecha.date() <= fifteen_days_later.date():
                        record["Dia"] = self.transformar_data(record["Dia"].split("/")[1])
                        # Combina os campos de TV
                        if record["Abierta_1"] and record["Cable_1"]:
                            record["Cable_1"] = f"{record['Abierta_1']} / {record['Cable_1']}"
                        elif record["Abierta_1"]:
                            record["Cable_1"] = record["Abierta_1"]
                        elif record["Cable_1"]:
                            record["Cable_1"] = record["Cable_1"]
                        if team_name:
                            similarity_a = fuzz.ratio(record["Equipo_A"].lower(), team_name.lower())
                            similarity_b = fuzz.ratio(record["Equipo_B"].lower(), team_name.lower())
                            if similarity_a >= similarity_threshold or similarity_b >= similarity_threshold:
                                filtered_data.append(record)
                        else:
                            filtered_data.append(record)

            return filtered_data
        except Exception as e:
            raise Exception(f"Erro ao recuperar dados do DuckDB: {str(e)}")
