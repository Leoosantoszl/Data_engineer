path_json = 'data_raw/dados_empresaA.json'
path_csv = 'data_raw/dados_empresaB.csv'

#Extract

dados_empresaA = Dados(path_json, 'json')
print(dados_empresaA.nome_colunas)
print(dados_empresaA.qtd_linhas)

dados_empresaB = Dados(path_csv, 'csv')
print(dados_empresaB.nome_colunas)
print(dados_empresaB.qtd_linhas)

#Transform

key_mapping = {'Nome do Item': 'Nome do Produto',
                'Classificação do Produto': 'Categoria do Produto',
                'Valor em Reais (R$)': 'Preço do Produto (R$)',
                'Quantidade em Estoque': 'Quantidade em Estoque',
                'Nome da Loja': 'Filial',
                'Data da Venda': 'Data da Venda'}

dados_empresaB.rename_columns(key_mapping)
print(dados_empresaB.nome_colunas)

dados_fusao = Dados.join(dados_empresaA, dados_empresaB)
print(dados_fusao.nome_colunas)
print(dados_fusao.qtd_linhas)

#Load

path_dados_combinados = 'data_processed/dados_combinados.csv'
dados_fusao.salvando_dados(path_dados_combinados)
print(path_dados_combinados)


import json
import csv

class Dados:

    def __init__(self, path, tipo):
        self.tipo = tipo
        self.path = path
        self.dados = self.leitura_dados()
        self.nome_colunas = self.get_columns()
        self.qtd_linhas = self.size_data()


    def leitura_json():
        dados_json = []
        with open(self.path, 'r') as file:
            dados_json = json.load(file)
        return dados_json

    def leitura_csv():

        dados_csv = []
        with open(self.path, 'r') as file:
            spamreader = csv.DictReader(file, delimiter=',')
            for row in spamreader:
                dados_csv.append(row)

        return dados_csv

    
    def leitura_dados(cls, path, tipo_dados):
        dados = []

        if self.tipo_dados == 'csv':
            dados = cls.__leitura_csv()
        
        elif self.tipo_dados == 'json':
            dados = cls.__leitura_json()

        self.dados = dados

    def get_columns(self):
        return list(self.dados[-1].keys())

    def rename_columns(self, key_mapping):
        new_dados = []

        for old_dict in self.dados:
            dict_temp = {}
            for old_key, value in old_dict.items():
                dict_temp[key_mapping[old_key]] = value
            new_dados.append(dict_temp)
        
        self.dados = new_dados
        self.nome_colunas = self.__get_columns()

    def size_data(self):
        return len(self.dados)

    def join(dadosA, dadosB):
        combined_list = []
        combined_list.extend(dadosA.dados)
        combined_list.extend(dadosB.dados)
        
        return Dados(combined_list)

        
    def transformando_dados_tabela(self):
        
        dados_combinados_tabela = [self.nome_colunas]

        for row in self.dados:
            linha = []
            for coluna in self.nome_colunas:
                linha.append(row.get(coluna, 'Indisponivel'))
            dados_combinados_tabela.append(linha)
        
        return dados_combinados_tabela

    def salvando_dados(self, path):

        dados_combinados_tabela = self.transformando_dados_tabela()

        with open(path, 'w') as file:
            writer = csv.writer(file)
            writer.writerows(dados_combinados_tabela)
