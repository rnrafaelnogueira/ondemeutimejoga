import pdfplumber
import duckdb
from typing import List, Dict
from fuzzywuzzy import fuzz  # Importando a biblioteca para calcular similaridade

class FutebolCopaNordesteService:

    def __init__(self, db_path):
        self.db_path = db_path

    # Função para transformar a string
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
        # Divide a string em data e dia da semana
        data, dia_abreviado = data_str.split()
        
        # Obtém o nome completo do dia da semana
        dia_completo = dias_semana.get(dia_abreviado, dia_abreviado)  # Usa a abreviação se não encontrar no dicionário
        
        # Retorna a string formatada
        return f"{data} - {dia_completo}"

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
                            if len(row) >= 12:  # Verifica se a linha tem colunas suficientes
                                record = {
                                    "Data": row[2] if row[2] else None,  
                                    "Hora": row[4] if row[4] else None,  
                                    "Jogo": row[6] if row[6] else None,
                                    "Estadio": row[7] if row[7] else None,
                                    "Cidade": row[8] if row[8] else None,
                                    "UF": row[9] if row[9] else None,
                                    "TV_1": row[10] if row[10] else None,
                                    "TV_2": row[11] if row[11] else None,
                                    "TV_3": row[12] if row[12] else None,
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
                    Data TEXT,
                    Hora TEXT,
                    Jogo TEXT,
                    Estadio TEXT,
                    Cidade TEXT,
                    UF TEXT,
                    TV_1 TEXT,
                    TV_2 TEXT,
                    TV_3 TEXT
                )
            """)
            # Insere cada registro na tabela
            for record in data:
                conn.execute(f"""
                    INSERT INTO {table_name} (
                       Data, Hora, Jogo, Estadio, Cidade, UF, TV_1,TV_2,TV_3
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    record["Data"], record["Hora"], record["Jogo"], record["Estadio"], record["Cidade"], record["UF"],
                    record["TV_1"], record["TV_2"], record["TV_3"]
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
            result = conn.execute(f"SELECT * FROM {table_name} WHERE Hora<>'HORA'").fetchall()
            conn.close()

            # Converte os registros em dicionários
            columns = [
                "Data", "Hora", "Jogo", "Estadio", "Cidade", "UF", "TV_1", "TV_2", "TV_3"
            ]
            data = [dict(zip(columns, row)) for row in result]

            # Mapeamento dos canais de TV
            tv_mapping = {
                "1": "SBT",
                "2": "Premiere",
                "3": "ESPN"
            }

            # Filtra os dados com base na similaridade do nome da equipe, se fornecido
            filtered_data = []
            last_data = None  # Variável para armazenar a última data válida

            for record in data:
                # Formata o campo "Data" para replicar o valor anterior se for null
                if record["Data"]:
                    # Atualiza a última data válida
                    last_data = record["Data"].split("\n")[0]  # Pega a primeira ocorrência da data
                    record["Data"] = self.transformar_data(last_data)
                else:
                    # Replica a última data válida
                    record["Data"] = self.transformar_data(last_data)

                # Mapeia os canais de TV
                for tv_key in ["TV_1", "TV_2", "TV_3"]:
                    if record[tv_key] in tv_mapping:
                        record[tv_key] = tv_mapping[record[tv_key]]

                # Filtra por similaridade do nome da equipe
                if team_name:
                    jogo = record.get("Jogo", "")
                    if jogo:
                        # Calcula a similaridade entre o nome da equipe e o campo "Jogo"
                        similarity = fuzz.partial_ratio(team_name.lower(), jogo.lower())
                        if similarity >= similarity_threshold:
                            filtered_data.append(record)
                else:
                    filtered_data.append(record)

            return filtered_data
        except Exception as e:
            raise Exception(f"Erro ao recuperar dados do DuckDB: {str(e)}")