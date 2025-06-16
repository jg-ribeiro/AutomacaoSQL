# SQL Job Scheduler & Exporter

Esta aplicação permite criar, gerenciar e agendar trabalhos (queries) SQL que são executadas em um banco de dados Oracle. Os resultados são exportados para arquivos CSV em um local especificado pelo usuário. A aplicação também expõe uma API para acessar os dados dos CSVs gerados.

A interface de gerenciamento é acessada via web (React), com um backend em Flask que gerencia as configurações dos jobs, usuários e autenticação.

## Tabela de Conteúdos
- [Principais Funcionalidades](#principais-funcionalidades)
- [Arquitetura da Aplicação](#arquitetura-da-aplicação)
- [Stack de Tecnologias](#stack-de-tecnologias)
- [Pré-requisitos](#pré-requisitos)
- [Instalação e Configuração](#instalação-e-configuração)
- [Como Executar](#como-executar)
- [Estrutura do `datafile.json`](#estrutura-do-datafilejson)
- [Endpoints da API](#endpoints-da-api)
- [Logging](#logging)

## Principais Funcionalidades

- **Gerenciamento de Jobs via Web**: Crie, edite, ative/desative e exclua jobs através de uma interface de usuário amigável.
- **Agendamento Flexível**: Agende a execução dos jobs para dias da semana, horas e minutos específicos.
- **Exportação para CSV**: Exporta o resultado de queries `SELECT` para arquivos CSV.
- **Conexão com Oracle**: Executa as queries em um banco de dados Oracle.
- **Armazenamento de Metadados**: Utiliza um banco de dados PostgreSQL para salvar as configurações dos jobs, logs e usuários.
- **Autenticação e Permissões**: Sistema de login com papéis de usuário (`root`, `moderator`, `normal`) para controle de acesso.
- **API de Dados**: Uma API segura para acessar os dados dos arquivos CSV gerados, com suporte a chaves de API.
- **Logging Detalhado**: Registra eventos importantes, erros e execuções de jobs tanto no console quanto no banco de dados PostgreSQL para auditoria.

## Arquitetura da Aplicação

A aplicação possui uma arquitetura peculiar que requer dois processos principais rodando simultaneamente para sua funcionalidade completa:

1.  **`backend.py` (Servidor Flask)**:
    - Fornece a API REST para o frontend (React).
    - Gerencia as operações de CRUD (Criar, Ler, Atualizar, Deletar) para os jobs, parâmetros e usuários.
    - Controla a autenticação e autorização.
    - Todas as configurações de jobs são salvas no banco de dados PostgreSQL.

2.  **`schedule.py` (Agendador)**:
    - Lê as configurações dos jobs ativos do banco de dados PostgreSQL.
    - Agenda a execução das tarefas nos horários definidos.
    - Executa as queries SQL no banco de dados Oracle.
    - Exporta os resultados para os arquivos CSV.
    - Recarrega a lista de jobs periodicamente para aplicar novas configurações sem precisar reiniciar o serviço.

**Diagrama de Fluxo Simplificado:**

```
[ Usuário ] <--> [ Frontend (React) ] <--> [ Backend (Flask) ]
                                                   ^
                                                   |
                                           [ Banco PostgreSQL ]
                                           (Jobs, Usuários, Logs)
                                                   ^
                                                   |
[ Agendador (schedule.py) ] --- (Lê jobs) --------'
       |
       '---- (Executa query) ---> [ Banco Oracle ] ---> (Gera) ---> [ Arquivos CSV ]
```

> **Importante**: O `backend.py` pode rodar sozinho, mas os jobs não serão executados. O `schedule.py` pode rodar sozinho, mas não receberá atualizações (novos jobs ou mudanças nos existentes) até ser reiniciado. **Para a operação correta, ambos devem estar em execução.**

## Stack de Tecnologias

- **Backend**: Python 3.9, Flask, SQLAlchemy
- **Banco de Dados de Metadados**: PostgreSQL
- **Banco de Dados de Origem**: Oracle
- **Agendamento**: `schedule`
- **Frontend**: React (servido pelo Flask)
- **Manipulação de Dados**: Pandas, oracledb

## Pré-requisitos

Antes de começar, certifique-se de que você tem os seguintes softwares instalados:
- Python 3.9
- Git
- Um servidor PostgreSQL acessível.
- **Oracle Instant Client**: O `schedule.py` depende do Oracle Instant Client para se conectar ao banco de dados Oracle. Certifique-se de que ele está instalado e o caminho para a biblioteca está corretamente configurado no `datafile.json`.

## Instalação e Configuração

Siga os passos abaixo para configurar o ambiente de desenvolvimento.

**1. Clone o repositório:**
```bash
git clone https://github.com/jg-ribeiro/AutomacaoSQL.git
cd AutomacaoSQL
```

**2. Crie e ative um ambiente virtual (recomendado):**
```bash
# Para Linux/macOS
python3 -m venv venv
source venv/bin/activate

# Para Windows
python -m venv venv
.\venv\Scripts\activate
```

**3. Instale as dependências:**
```bash
pip install -r requirements.txt
```
> **Nota sobre o `numpy`**: O arquivo `requirements.txt` especifica uma versão `~1.26.4`. Se precisar modificar as dependências, garanta que a versão do `numpy` permaneça na faixa `1.x`, pois pode ser um requisito específico do projeto.

**4. Crie e configure o `datafile.json`:**
A primeira vez que você executar `auxiliares.py` ou o `backend.py`, um arquivo `datafile.json` será criado na raiz do projeto com uma estrutura base. Você **precisa** preencher este arquivo com as suas credenciais e caminhos corretos.

```bash
# Executar este comando irá criar o arquivo se ele não existir
python auxiliares.py
```
Depois, edite o `datafile.json`. Veja a seção [Estrutura do datafile.json](#estrutura-do-datafilejson) para mais detalhes.

**5. Crie as tabelas no banco de dados PostgreSQL:**
O backend usa SQLAlchemy para mapear os modelos para tabelas. Para criar as tabelas pela primeira vez:
- Abra o arquivo `backend.py`.
- No final do arquivo, descomente a linha `db.create_all()`.
- Execute o backend uma vez: `python backend.py`.
- As tabelas (`users`, `jobs_he`, `jobs_de`, `parameters`, `logs`) serão criadas no seu banco PostgreSQL.
- **Comente a linha `db.create_all()` novamente** para evitar problemas futuros.

## Como Executar

Para rodar a aplicação, você precisa abrir dois terminais separados, ambos com o ambiente virtual ativado.

**No Terminal 1 - Inicie o Backend:**
```bash
python backend.py
```
O servidor Flask estará rodando, geralmente em `http://0.0.0.0:5000`. A interface web estará acessível em `http://localhost:5000`.

**No Terminal 2 - Inicie o Agendador:**
```bash
python schedule.py
```
O agendador começará a rodar em segundo plano, carregando os jobs do banco de dados e esperando os horários para executá-los.

## Estrutura do `datafile.json`

Este arquivo centraliza todas as configurações sensíveis e específicas do ambiente.

```json
{
  "oracle_database": {
    "TSN": "seu_tns_name_aqui",
    "INSTANT_CLIENT": "C:/path/to/your/oracle/instantclient"
  },
  "backend": {
    "secret_key": "uma-chave-secreta-muito-forte-aqui"
  },
  "postgres":{
    "hostname": "localhost",
    "port": "5432",
    "database": "nome_do_banco",
    "username": "usuario_postgres",
    "password": "senha_postgres"
  },
  "user_name": "usuario_oracle",
  "user_pass": "senha_oracle",
  "data_api": {
    "csv_folder_path": "C:/caminho/para/pasta/dos/csvs",
    "api_keys": [
      "uma-chave-de-api-segura",
      "outra-chave-se-necessario"
    ]
  }
}
```
- `oracle_database.TSN`: O TNS Name ou a string de conexão completa do Oracle.
- `oracle_database.INSTANT_CLIENT`: O caminho absoluto para a pasta do Oracle Instant Client.
- `backend.secret_key`: Chave secreta para as sessões do Flask.
- `postgres`: Credenciais para a conexão com o banco de dados PostgreSQL.
- `user_name`, `user_pass`: Credenciais do usuário Oracle que será usado para executar as queries.
- `data_api.csv_folder_path`: Caminho absoluto para a pasta onde os CSVs serão salvos e de onde a API de dados irá lê-los.
- `data_api.api_keys`: Uma lista de chaves de API válidas para acessar a API de dados.

## Endpoints da API

O backend expõe vários endpoints. Aqui estão alguns dos principais:

| Método | Endpoint                    | Autenticação       | Descrição                                         |
|--------|-----------------------------|--------------------|-----------------------------------------------------|
| `POST` | `/api/login`                | Pública            | Autentica um usuário e cria uma sessão.             |
| `POST` | `/api/logout`               | Requer Login       | Desloga o usuário atual.                            |
| `GET`  | `/api/me`                   | Pública            | Retorna o status de autenticação do usuário atual.  |
| `GET`  | `/api/jobs`                 | Requer Login       | Lista todos os jobs configurados.                   |
| `POST` | `/api/jobs`                 | Requer Login       | Cria um novo job.                                   |
| `PUT`  | `/api/jobs/<int:job_id>`    | Requer Login       | Atualiza um job existente.                          |
| `DELETE`| `/api/jobs/<int:job_id>`   | Requer Login       | Deleta um job.                                      |
| `GET`  | `/api/users`                | Papel: `root`      | Lista todos os usuários.                            |
| `POST` | `/api/users`                | Papel: `root`      | Cria um novo usuário.                               |
| `GET`  | `/api/data/datasets`        | Chave de API / Login | Lista os arquivos CSV disponíveis para a API de dados. |
| `GET`  | `/api/data/datasets/<path>` | Chave de API / Login | Retorna o conteúdo de um CSV como JSON.             |

## Logging

A aplicação utiliza um sistema de logging centralizado (`logging_config.py`):
- **Console**: Logs de DEBUG, INFO, WARNING e ERROR são exibidos no console onde o `backend.py` e o `schedule.py` estão rodando.
- **Banco de Dados (PostgreSQL)**: Logs de INFO, WARNING e ERROR são salvos na tabela `logs` para auditoria e análise posterior. Isso inclui informações sobre execuções de jobs, erros, logins de usuários e outras ações importantes.
