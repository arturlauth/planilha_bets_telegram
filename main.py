import csv
import os
import re
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes
from typing import Final
from datetime import datetime
import gspread
from google.oauth2 import service_account
import logging
import asyncio

# Configuração básica do logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)
logger = logging.getLogger(__name__)

#env para koeyb
TOKEN = os.environ.get("token")
BOT_USERNAME = os.environ.get("bot_username")
private_key_id = os.environ.get("private_key_id")
private_key = os.environ.get("private_key")
client_email = os.environ.get("client_email")
client_id = os.environ.get("client_id")
auth_uri = os.environ.get("auth_uri")
token_uri = os.environ.get("token_uri")
auth_provider_x509_cert_url = os.environ.get("auth_provider_x509_cert_url")
client_x509_cert_url = os.environ.get("client_x509_cert_url")

# TOKEN = os.environ['token']
# BOT_USERNAME = os.environ['bot_username']
# # Acessar as variáveis de ambiente


# private_key_id = os.environ['private_key_id']
# private_key = os.environ['private_key']
# client_email = os.environ['client_email']
# client_id = os.environ['client_id']
# auth_uri = os.environ['auth_uri']
# token_uri = os.environ['token_uri']
# auth_provider_x509_cert_url = os.environ['auth_provider_x509_cert_url']
# client_x509_cert_url = os.environ['client_x509_cert_url']

# Exemplo de como usar as credenciais
credentials = {
    "type": "service_account",
    "project_id": "telegram-planilha-bot",
    "private_key_id": private_key_id,
    "private_key": private_key,
    "client_email": client_email,
    "client_id": client_id,
    "auth_uri": auth_uri,
    "token_uri": token_uri,
    "auth_provider_x509_cert_url": auth_provider_x509_cert_url,
    "client_x509_cert_url": client_x509_cert_url,
    "universe_domain": "googleapis.com"
}

CHAT_PRIVADO_ID: Final[int] = 6302648701  # Substitua pelo ID real do chat privado

# Defina o caminho completo para onde o arquivo será salvo no seu computador
FILE_PATH = r"D:\Documentos\pythonprojects\telegram_bot\output.csv"

# ID PLANILHA
sheet_id: Final[str] = "1FQFPoZO2LCuTpL1LWy5IfXzUm5qVeWhvo_OSuSbUc60"
# ID PLANILHA 2
sheet_id_2: Final[str] = "11ielWFfC_qBeEYG15fRT8s_Btri70FJ5BxX1ffaoRQc"
# Autenticação e acesso à planilha do Google Sheets
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SERVICE_ACCOUNT_FILE = credentials

# Exemplo de dados a serem inseridos
data = ['GRUPO', 'BET', 'HOME', 'AWAY', 'DATA', 'ODDS', 'DATA_ENVIO']
sheet_range = 'A1:G1'


def authenticate_google_sheets():
    try:
        creds = service_account.Credentials.from_service_account_info(credentials, scopes=SCOPES)
        client = gspread.authorize(creds)
        logger.info("Autenticação no Google Sheets realizada com sucesso.")
        return client
    except Exception as e:
        logger.error(f"Erro na autenticação do Google Sheets: {e}")
        return None
def deduplica_google_sheet():
    sheet = client.open_by_key(sheet_id)
    worksheet = sheet.sheet1  # Seleciona a primeira aba da planilha
    # Passo 1: Obter todas as linhas da planilha
    all_rows = worksheet.get_all_values()

    # Passo 2: Identificar e remover duplicatas
    seen = set()  # Conjunto para identificar linhas duplicadas
    deduplicated_rows = []

    # Mantém as linhas únicas
    for row in all_rows:
        row_tuple = tuple(row)  # Converte a linha em uma tupla para ser imutável
        if row_tuple not in seen:  # Se a linha não foi vista, adiciona ao conjunto e à lista de linhas sem duplicatas
            deduplicated_rows.append(row)
            seen.add(row_tuple)

    # Passo 3: Limpar a planilha atual (opcional, se você deseja sobrescrever)
    worksheet.clear()

    # Passo 4: Atualizar a planilha com as linhas sem duplicatas
    worksheet.update('A1', deduplicated_rows)

    print("Linhas duplicadas removidas com sucesso!")


def update_google_sheet(data, sheet_id, sheet_range):
    try:
        # Autentica e acessa a planilha
        client = authenticate_google_sheets()
        if client:
            # Abre a planilha usando o ID
            sheet = client.open_by_key(sheet_id)
            worksheet = sheet.sheet1  # ou sheet.worksheet('Nome da Aba')

            # Verifica se a planilha está vazia (ou se a primeira linha está vazia)
            first_row = worksheet.row_values(1)  # Obtenha a primeira linha
            if not first_row:  # Se estiver vazia, adicione o cabeçalho
                header = ['GRUPO', 'BET', 'MERCADO', 'HOME', 'AWAY', 'DATA', 'ODDS', 'DATA_ENVIO']
                worksheet.update('A1', [header])

            # Transforma os dados do dicionário em uma lista na ordem correta
            row = [
                data['GRUPO'],
                data['BET'],
                data['MERCADO'],
                data['HOME'],
                data['AWAY'],
                data['DATA'],
                data['ODDS'],
                data['DATA_ENVIO']
            ]

            # Escreve os dados na aba selecionada
            worksheet.append_row(row)  # Adiciona os dados como uma nova linha

            print('Dados atualizados na planilha do Google Sheets com sucesso.')
        else:
            print('Erro: Autenticação falhou.')
    except Exception as e:
        print(f'Erro ao atualizar a planilha do Google Sheets: {e}')
# Função para formatar a data no formato dd/mm/yyyy
def format_date(date_str: str) -> str:
    try:
        clean_date_str = date_str.split(' (')[0].strip()
        parsed_date = datetime.strptime(clean_date_str, '%a %b %d %Y %H:%M:%S GMT%z')
        return parsed_date.strftime('%d/%m/%Y')
    except ValueError as e:
        logger.warning(f"Erro ao formatar a data: {e}. Data original: {date_str}")
        return date_str


# Função para extrair a data de envio da mensagem no formato dd/mm/yyyy
def format_message_date(message_date: datetime) -> str:
    return message_date.strftime('%d/%m/%Y')


# Função para processar a mensagem e extrair as informações
def process_message(text: str, message_date: datetime) -> dict:
    try:
        # Colocar todo o texto em minúsculo
        text = text.lower()

        # Remover a linha "fair odds" da variável text
        lines = text.split('\n')
        lines = [line for line in lines if not line.startswith("fair odds:")]

        # Recriar a variável text sem a linha "fair odds"
        text = '\n'.join(lines).strip()

        # Processar as linhas restantes
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        logger.info(f"Linhas processadas da mensagem: {lines}")

        # Padrão 1: "very big" (double chance)
        if "very big" in lines[0]:
            grupo = lines[0]  # Primeira linha completa
            home = lines[1].split('-', 1)[-1].strip()  # Texto após o primeiro "-" na linha 2
            if "double chance" in grupo:
                mercado = "dc"
            else:
                mercado = "ml"
            bet = "home"
            away = lines[2].split('-', 1)[-1].strip()  # Texto após o primeiro "-" na linha 3
            odds = lines[5].split('-', 1)[-1].strip()  # Texto após o primeiro "-" na linha 6
            date = format_date(lines[4].split('-', 1)[-1].strip())  # Data formatada da linha 5
            data_envio = format_message_date(message_date)  # Data de envio da mensagem
            grupo = "very big"
            return {
                'GRUPO': grupo,
                'BET': bet,
                'MERCADO': mercado,
                'HOME': home,
                'AWAY': away,
                'DATA': date,
                'ODDS': odds,
                'DATA_ENVIO': data_envio
            }
        # padrao 1 under:
        elif 'Under' in linhas[0]:
            grupo = 'under'
            bet_match = re.search(r'Under \d+\.\d+', linhas[0])
            bet = bet_match.group(0) if bet_match else ''
        
            mercado = 'under'

            # Extração de Home e Away
            home = re.search(r'Home Name - (.+)', mensagem).group(1)
            away = re.search(r'Away Name - (.+)', mensagem).group(1)

            # Extração de data e formatação
            data_kickoff = re.search(r'Kick off - (.+)', mensagem).group(1)
            data_obj = datetime.strptime(data_kickoff, "%a %b %d %Y %H:%M:%S GMT%z")
            data_formatada = data_obj.strftime("%d/%m/%Y")

            return {
                'grupo': grupo,
                'bet': bet,
                'mercado': mercado,
                'home': home,
                'away': away,
                'data': data_formatada,
            }
        # Padrão 2: "under" (outro formato)
        elif "king" in lines[0]:
            grupo = lines[0]
            bet = lines[1].split('-', 1)[-1].strip()  # Texto após o primeiro "-" na linha 2
            mercado = "ml"
            match = lines[3].split('-', 1)[-1].strip()
            if "home" in match:
                match = "home"
            elif "away" in match:
                match = "away"
            home, away = match.split(' v ')
            odds = lines[6].split('-', 1)[-1].strip()
            date = format_date(lines[5].split('-', 1)[-1].strip())
            data_envio = format_message_date(message_date)
            grupo = "under"
            return {
                'GRUPO': grupo,
                'BET': bet,
                'MERCADO': mercado,
                'HOME': home.strip(),
                'AWAY': away.strip(),
                'DATA': date,
                'ODDS': odds,
                'DATA_ENVIO': data_envio
            }

        # Padrão 3: "home draw"
        elif "home draw" in lines[0]:
            grupo = lines[0].split(' - ')[0].strip()
            bet = "home"
            mercado = "dnb"
            home = lines[1].split('-', 1)[-1].strip()
            away = lines[2].split('-', 1)[-1].strip()
            odds = lines[5].split('-', 1)[-1].strip()
            date = format_date(lines[4].split('-', 1)[-1].strip())
            data_envio = format_message_date(message_date)
            grupo = "king ml"
            return {
                'GRUPO': grupo,
                'BET': bet,
                'MERCADO': mercado,
                'HOME': home.strip(),
                'AWAY': away.strip(),
                'DATA': date,
                'ODDS': odds,
                'DATA_ENVIO': data_envio
            }

        # Padrão 4: Novo formato (⚽️ football ⚽️)
        elif "football" in lines[0] or "basketball" in lines[0]:
            grupo = lines[0]
            if "football" in grupo:
                grupo = "palerts football"
            elif "basketball" in grupo:
                grupo = "basketball best filter"
            home = lines[2].split(' vs ')[0].strip()
            away = lines[2].split(' vs ')[1].strip()
            data_jogo = lines[4].strip()
            mercado = lines[5].strip()  # Pode ser "spread", "moneyline", etc.
            bet = lines[7].split('@')[0].strip()  # Bet sem odds
            odds = lines[7].split('@')[1].split('(')[0].strip()
            data_formatada = datetime.strptime(data_jogo, "%d.%m.%Y-%H:%M").strftime('%d/%m/%Y')
            data_envio = format_message_date(message_date)

            return {
                'GRUPO': grupo,
                'BET': bet,
                'MERCADO': mercado,
                'HOME': home,
                'AWAY': away,
                'DATA': data_formatada,
                'ODDS': odds,
                'DATA_ENVIO': data_envio
            }

        # Verifica se a lista 'lines' tem pelo menos uma linha
        elif len(lines) > 0 and ("home" in lines[0] or "away" in lines[0]):
            # BET: 'home' se houver "home" na primeira linha, caso contrário, 'away'
            bet = 'home' if 'home' in lines[0] else 'away'
            grupo = 'ml'
            # MERCADO: 'dnb' se houver "draw" na primeira linha, caso contrário, 'ml'
            mercado = 'dnb' if 'draw' in lines[0] else 'ml'
            # HOME: encontra o nome do time após "home name" ou similar
            home_match = re.search(r'home name[^\w]*(.*)', text)
            home = home_match.group(1).strip() if home_match else 'n/a'
            # AWAY: encontra o nome do time após "away name" ou similar
            away_match = re.search(r'away name[^\w]*(.*)', text)
            away = away_match.group(1).strip() if away_match else 'n/a'
            # DATA: encontra a data no formato correto
            date_match = re.search(r'date[^\w]*(.*)', text)
            if date_match:
                raw_date = date_match.group(1).strip()
                try:
                    # Tentativa de parse com o formato '2024-10-06 18:30'
                    date = datetime.strptime(raw_date, '%Y-%m-%d %H:%M').strftime('%d/%m/%Y')
                except ValueError:
                    # Tenta outro formato de data (exemplo com GMT)
                    try:
                        date = datetime.strptime(raw_date.split(' gmt')[0].strip(),
                                                 '%a %b %d %Y %H:%M:%S').strftime('%d/%m/%Y')
                    except ValueError:
                        date = 'n/a'
            else:
                date = 'n/a'
            # ODDS: encontra o valor de odds
            odds_match = re.search(r'odds[^\w]*(.*)', text)
            odds = odds_match.group(1) if odds_match else 'n/a'
            # DATA_ENVIO: data de recebimento da mensagem
            data_envio = message_date.strftime('%d/%m/%Y')

            return {
                'GRUPO': grupo,
                'BET': bet,
                'MERCADO': mercado,
                'HOME': home,
                'AWAY': away,
                'DATA': date,
                'ODDS': odds,
                'DATA_ENVIO': data_envio
            }

        return None  # Se o formato não for compatível
    except Exception as e:
        logger.error(f"Erro ao processar a mensagem: {e}")
        return None


# Função para salvar os dados extraídos no CSV
def save_to_csv(data: dict, filename=FILE_PATH):
    try:
        write_header = not os.path.exists(filename)
        with open(filename, mode='a', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=['GRUPO', 'BET', 'MERCADO', 'HOME', 'AWAY', 'DATA', 'ODDS',
                                                      'DATA_ENVIO'])
            if write_header:
                writer.writeheader()
            writer.writerow(data)
        logger.info(f"Dados {data} salvos no arquivo CSV.")
    except Exception as e:
        logger.error(f"Erro ao salvar dados no CSV: {e}")


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        # Verifica se a mensagem é de grupo ou de canal
        if update.channel_post:
            message_type = 'channel'
            message_date = update.channel_post.date
            text = update.channel_post.text

            logger.info(f"Mensagem recebida de {update.channel_post.chat.id} em {message_type}: {text}")

        elif update.message:
            message_type = update.message.chat.type
            text = update.message.text
            message_date = update.message.date

            logger.info(f"Mensagem recebida de {update.message.chat.id} em {message_type}: {text}")

        else:
            logger.warning("Mensagem recebida não é de grupo nem de canal.")
            return  # Se não for nem mensagem de grupo nem de canal, sai da função

        # Processa a mensagem
        logger.info("Iniciando o processamento da mensagem.")
        extracted_data = process_message(text, message_date)

        logger.info(f"Dados extraídos: {extracted_data}")

        if extracted_data:
            logger.info("Dados extraídos com sucesso. Salvando dados...")
            # save_to_csv(extracted_data)
            update_google_sheet(extracted_data, sheet_id, sheet_range)
            update_google_sheet(extracted_data, sheet_id_2, sheet_range)

            # Enviar confirmação para o chat privado
            await context.bot.send_message(
                chat_id=CHAT_PRIVADO_ID,
                text=f'Dados salvos com sucesso:\n{extracted_data}'
            )
            logger.info("Confirmação enviada para o chat privado.")
        else:
            logger.error("Formato de mensagem inválido. Nenhum dado foi extraído.")
            # Enviar mensagem de erro no chat privado
            await context.bot.send_message(
                chat_id=CHAT_PRIVADO_ID,
                text='Formato de mensagem inválido.'
            )
            logger.info("Mensagem de erro enviada para o chat privado.")

    except Exception as e:
        logger.exception("Erro ao processar a mensagem: %s", str(e))

async def get_chat_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.message.chat.id
    await update.message.reply_text(f'O ID do chat privado é: {chat_id}')

# Comando /start
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text('Bot iniciado! Envie mensagens no formato correto para salvá-las no CSV.')


# Comando customizado
async def custom_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text('Este é um comando customizado.')


# Tratamento de erros
async def error(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Update {update} causou um erro: {context.error}")

async def start_bot():
    app = ApplicationBuilder().token(TOKEN).build()

    # Adicionar handlers
    app.add_handler(CommandHandler('start', start_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    # Configurar Webhook
    webhook_url = "https://your-webhook-url.com/webhook"
    await app.bot.set_webhook(webhook_url)

    # Iniciar o bot com webhook
    await app.start()  # Inicia o bot
    print("Webhook configurado e bot rodando!")

    # Manter o bot ativo
    await app.idle()  # use idle() para manter o bot ativo

def main():
    # Usar asyncio.run para executar a função assíncrona
    asyncio.run(start_bot())

if __name__ == '__main__':
    main()
