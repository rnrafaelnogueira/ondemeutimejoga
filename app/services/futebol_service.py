import pdfplumber
import duckdb
from typing import List, Dict
from fuzzywuzzy import fuzz  # Importando a biblioteca para calcular similaridade

class FutebolService:

    def __init__(self, db_path):
        self.db_path = db_path

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
            if team_name:
                for record in data:
                    if record["Equipo_A"] and record["Equipo_B"]:
                        record["Dia"] = record["Dia"].split("/")[1]
                        # Calcula a similaridade entre o nome da equipe A e o nome fornecido
                        similarity_a = fuzz.ratio(record["Equipo_A"].lower(), team_name.lower())
                        # Calcula a similaridade entre o nome da equipe B e o nome fornecido
                        similarity_b = fuzz.ratio(record["Equipo_B"].lower(), team_name.lower())
                        # Se a similaridade for maior ou igual ao limite, adiciona ao resultado filtrado
                        if similarity_a >= similarity_threshold or similarity_b >= similarity_threshold:
                            filtered_data.append(record)
                return filtered_data
            else:
                for record in data:
                    if record["Equipo_A"] and record["Equipo_B"]:
                        record["Dia"] = record["Dia"].split("/")[1]
                        filtered_data.append(record)
                return filtered_data
        except Exception as e:
            raise Exception(f"Erro ao recuperar dados do DuckDB: {str(e)}")