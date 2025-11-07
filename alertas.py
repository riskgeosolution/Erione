# alertas.py (ATUALIZADO: Novos nomes de status "ALERTA MÁXIMO" e "OBSERVAÇÃO")

import httpx
import os
import requests
import json
import traceback  # Para logar falhas internas

# --- Constantes da API SMTP2GO ---
SMTP2GO_API_URL = "https://api.smtp2go.com/v3/email/send"

# --- Variáveis de Ambiente (E-MAIL) - LENDO DO OS.ENVIRON ---
SMTP2GO_API_KEY = os.environ.get('SMTP2GO_API_KEY')
SMTP2GO_SENDER_EMAIL = os.environ.get('SMTP2GO_SENDER_EMAIL')
DESTINATARIOS_EMAIL_STR = os.environ.get('DESTINATARIOS_EMAIL')

# --- Variáveis de Ambiente (SMS - COMTELE) - LENDO DO OS.ENVIRON ---
COMTELE_API_KEY = os.environ.get('COMTELE_API_KEY')
SMS_DESTINATARIOS_STR = os.environ.get('SMS_DESTINATARIOS')


# --- Função Helper de E-mail (SMTP2GO) (ATUALIZADO) ---
def _enviar_email_smtp2go(api_key, sender_email, recipients_list, subject, html_body):
    """ Envia um e-mail usando a API HTTP da SMTP2GO. """

    cor_css = "grey"
    # --- INÍCIO DA ATUALIZAÇÃO ---
    if 'ALERTA MÁXIMO' in subject.upper():
        cor_css = "#dc3545"
    elif 'OBSERVAÇÃO' in subject.upper():
        cor_css = "#28a745"
    # --- FIM DA ATUALIZAÇÃO ---

    payload = {
        "api_key": api_key,
        "sender": sender_email,
        "to": recipients_list,
        "subject": subject,
        "html_body": f"""
            <html>
            <body style="font-family: Arial, sans-serif; margin: 20px;">
                <h1 style='color: {cor_css};'>Alerta de Risco: {subject.split(':')[-1].strip()}</h1>
                <p>{html_body}</p>
                <p style="font-size: 0.8em; color: #777;">Este é um e-mail automático.</p>
            </body>
            </html>
            """,
        "text_body": "Por favor, habilite o HTML para ver esta mensagem de alerta."
    }
    headers = {"Content-Type": "application/json"}

    try:
        with httpx.Client(timeout=10.0) as client:
            response = client.post(SMTP2GO_API_URL, headers=headers, json=payload)

        # Tratamento de erro SMTP2GO: Não levanta exceção, apenas loga.
        if response.status_code == 200 and response.json().get('data', {}).get('failures', 1) == 0:
            print(f"E-mail de alerta (SMTP2GO) enviado com sucesso para: {recipients_list}")
        else:
            print(f"ERRO API SMTP2GO (E-mail): {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"ERRO DE CONEXÃO/REQUISIÇÃO (E-mail): {e}")
        return False
    return True


# --- Função Helper de SMS (COMTELE - SEM raise Exception) ---
def _enviar_sms_comtele(api_key, recipients_list, message):
    """ Envia SMS usando a API da Comtele e imprime a resposta detalhada. """
    COMTELE_API_URL = "https://sms.comtele.com.br/api/v2/send"

    if not api_key: return False

    numeros_com_virgula = ",".join(recipients_list)

    payload = {"Content": message, "Receivers": numeros_com_virgula}

    headers = {"auth-key": api_key, "Content-Type": "application/json"}

    print(f"--- Tentando enviar SMS (Comtele) para: {numeros_com_virgula} ---")

    try:
        response = requests.post(COMTELE_API_URL, headers=headers, json=payload, timeout=10.0)

        try:
            # Verifica se a API retornou sucesso
            success = response.json().get('Success', False)
        except requests.exceptions.JSONDecodeError:
            success = False

        if response.status_code == 200 and success:
            print(f"SMS de alerta (Comtele) enviado com sucesso para: {numeros_com_virgula}")
            return True
        else:
            # Se falhou, APENAS LOGA O ERRO DETALHADO
            print(f"!!! FALHA NO ENVIO SMS (Comtele) !!! Status HTTP: {response.status_code}")
            print(f"Resposta API: {response.text}")
            return False

    except Exception as e:
        print(f"ERRO DE CONEXÃO/REQUISIÇÃO (Comtele SMS): {e}")
        return False


# --- FUNÇÃO PRINCIPAL UNIFICADA (ATUALIZADA) ---
def enviar_alerta(id_ponto, nome_ponto, novo_status, status_anterior):
    """
    Função principal de alerta. Tenta enviar E-mail e SMS de forma INDEPENDENTE.
    Não levanta exceção.
    """

    # --- INÍCIO DA ATUALIZAÇÃO (Novas transições e textos) ---
    if novo_status == "ALERTA MÁXIMO" and status_anterior == "ALERTA":
        assunto_email = f"ALERTA CRÍTICO: ALERTA MÁXIMO - {nome_ponto}"
        html_body_part = f"O ponto {nome_ponto} acaba de passar de ALERTA para a condição CRÍTICA de ALERTA MÁXIMO."
        sms_mensagem = f"ALERTA: {nome_ponto} passou para ALERTA MAXIMO. Acao e requerida."

    elif novo_status == "OBSERVAÇÃO" and status_anterior == "ATENÇÃO":
        assunto_email = f"AVISO: NORMALIZAÇÃO - {nome_ponto}"
        html_body_part = f"O ponto {nome_ponto} voltou de ATENÇÃO para a condição de OBSERVAÇÃO."
        sms_mensagem = f"AVISO: {nome_ponto} voltou para OBSERVACAO."
    # --- FIM DA ATUALIZAÇÃO ---

    else:
        # Se não for uma das transições críticas definidas, ignora
        return False  # Retorna False para indicar que nada foi enviado

    # Flags de sucesso
    sucesso_email = False
    sucesso_sms = False

    # 1. Envio de E-mail (Isolado)
    if SMTP2GO_API_KEY and SMTP2GO_SENDER_EMAIL and DESTINATARIOS_EMAIL_STR:
        destinatarios_email = [email.strip() for email in DESTINATARIOS_EMAIL_STR.split(',')]
        if destinatarios_email:
            sucesso_email = _enviar_email_smtp2go(SMTP2GO_API_KEY, SMTP2GO_SENDER_EMAIL, destinatarios_email,
                                                  assunto_email, html_body_part)
    else:
        print(f"AVISO: Envio de E-mail não configurado.")

    # 2. Envio de SMS (Comtele - Isolado)
    if COMTELE_API_KEY and SMS_DESTINATARIOS_STR:
        # Pega a string de números, remove espaços, mas envia a string formatada
        destinatarios_sms = [num.strip() for num in SMS_DESTINATARIOS_STR.split(',')]
        if destinatarios_sms:
            sucesso_sms = _enviar_sms_comtele(COMTELE_API_KEY, destinatarios_sms, sms_mensagem)
    else:
        print(f"AVISO: Envio de SMS não configurado.")

    # Retorna o status combinado (True se pelo menos um método funcionou, False caso contrário)
    return sucesso_email or sucesso_sms