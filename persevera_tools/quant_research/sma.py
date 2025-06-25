import pandas as pd
import numpy as np
import datetime
from typing import Dict, Any, List, Union, Literal

def simular_patrimonio(
    data_nascimento: Union[str, datetime.date],
    patrimonio_inicial: float,
    periodo_acumulacao: float,
    periodo_distribuicao: float,
    resgate_mensal: float,
    aporte_mensal: float,
    inflacao_esperada: float,
    rentabilidade_nominal_esperada: float,
    aliquota_irrf: float,
) -> pd.DataFrame:
    """
    Simula a evolução de um patrimônio ao longo do tempo.

    A simulação é dividida em duas fases: acumulação e distribuição.
    O imposto de renda incide sobre o ganho de capital no momento do resgate.

    Args:
        data_nascimento (Union[str, datetime.date]): Data de nascimento do titular.
        patrimonio_inicial (float): Valor inicial do patrimônio.
        periodo_acumulacao (float): Duração da fase de acumulação em anos.
        periodo_distribuicao (float): Duração da fase de distribuição em anos.
        resgate_mensal (float): Valor do resgate mensal bruto durante a distribuição.
        aporte_mensal (float): Valor do aporte mensal durante a acumulação.
        inflacao_esperada (float): Taxa de inflação anual esperada (em %).
        rentabilidade_nominal_esperada (float): Taxa de rentabilidade nominal anual esperada (em %).
        aliquota_irrf (float): Alíquota de imposto de renda sobre o rendimento (em %).

    Returns:
        pd.DataFrame: DataFrame com a projeção mensal do patrimônio.
    """
    data_nascimento = pd.to_datetime(data_nascimento)

    # Convertendo taxas anuais para mensais
    rentabilidade_mensal_bruta_taxa = (1 + rentabilidade_nominal_esperada / 100.0) ** (1/12) - 1
    inflacao_mensal_calc = (1 + inflacao_esperada / 100.0) ** (1/12) - 1
    
    meses_acumulacao = int(periodo_acumulacao * 12)
    meses_distribuicao = int(periodo_distribuicao * 12)
    meses_totais_simulacao = meses_acumulacao + meses_distribuicao

    datas_simulacao = pd.date_range(start=datetime.date.today(), periods=meses_totais_simulacao + 1, freq='MS')

    # Inicializando variáveis para armazenar resultados
    patrimonio_atual_para_prox_mes = patrimonio_inicial
    capital_investido = patrimonio_inicial
    imposto_pago_acumulado = 0.0
    resgate_total = 0.0
    aporte_total = 0.0
    rendimento_total = 0.0
    
    # Adicionar o estado inicial (Mês 0)
    resultados = [{
        "Data": datas_simulacao[0],
        "Patrimônio Inicial Mês": patrimonio_inicial,
        "Rendimento Mensal": 0.0,
        "Imposto Pago Mensal": 0.0,
        "Aporte Mensal Ajustado": 0.0,
        "Resgate Mensal Ajustado": 0.0,
        "Patrimônio Final Mês": patrimonio_inicial,
        "Rendimento Acumulado": rendimento_total,
        "Imposto Pago Acumulado": imposto_pago_acumulado,
        "Resgate Acumulado": resgate_total,
        "Aporte Acumulado": aporte_total,
        "Fator Inflação": 1.0,
        "Inflação Acumulada": 0.0
    }]
    
    # Simulação mês a mês
    for mes in range(1, meses_totais_simulacao + 1):
        patrimonio_inicial_mes_corrente = patrimonio_atual_para_prox_mes

        fator_inflacao = (1 + inflacao_mensal_calc) ** (mes - 1)

        # Se o patrimônio zerou, não há mais o que simular
        if patrimonio_inicial_mes_corrente <= 0:
            patrimonio_atual_para_prox_mes = 0
            rendimento_bruto_mes = 0.0
            imposto_do_mes = 0.0
            aporte_ajustado_mes_corrente = 0.0
            resgate_efetivo_mes = 0.0
        else:
            # Aplicar rendimento
            rendimento_bruto_mes = patrimonio_inicial_mes_corrente * rentabilidade_mensal_bruta_taxa
            rendimento_total += rendimento_bruto_mes
            
            aporte_do_mes_base = 0.0
            resgate_do_mes_base = 0.0

            if mes <= meses_acumulacao:
                # Fase de Acumulação
                aporte_do_mes_base = aporte_mensal
            else:
                # Fase de Distribuição (ocorre após os meses de acumulação)
                resgate_do_mes_base = resgate_mensal
            
            # Adicionar aportes (ajustados pela inflação)
            aporte_ajustado_mes_corrente = aporte_do_mes_base * fator_inflacao
            aporte_total += aporte_ajustado_mes_corrente
            capital_investido += aporte_ajustado_mes_corrente
            
            patrimonio_antes_mov = patrimonio_inicial_mes_corrente + rendimento_bruto_mes + aporte_ajustado_mes_corrente

            # Aplicar resgate ajustado pela inflação e calcular imposto
            resgate_desejado_mes = resgate_do_mes_base * fator_inflacao
            resgate_efetivo_mes = min(resgate_desejado_mes, patrimonio_antes_mov)
            resgate_total += resgate_efetivo_mes

            imposto_do_mes = 0.0
            if resgate_efetivo_mes > 0:
                ganho_de_capital_total = patrimonio_antes_mov - capital_investido
                
                if ganho_de_capital_total > 0 and patrimonio_antes_mov > 0:
                    proporcao_ganho = ganho_de_capital_total / patrimonio_antes_mov
                    ganho_realizado = resgate_efetivo_mes * proporcao_ganho
                    imposto_do_mes = max(0, ganho_realizado * (aliquota_irrf / 100.0))
                    
                    # O imposto também não pode ser maior que o patrimônio restante
                    imposto_do_mes = min(imposto_do_mes, patrimonio_antes_mov - resgate_efetivo_mes)

                    proporcao_capital = 1 - proporcao_ganho
                    capital_resgatado = resgate_efetivo_mes * proporcao_capital
                    capital_investido -= capital_resgatado
                else:
                    capital_investido -= resgate_efetivo_mes
            
            patrimonio_atual_para_prox_mes = patrimonio_antes_mov - resgate_efetivo_mes - imposto_do_mes
        
        imposto_pago_acumulado += imposto_do_mes
        patrimonio_final_mes_corrente = max(0, patrimonio_atual_para_prox_mes)
        
        # Armazenar resultado para cada mês
        resultados.append({
            "Data": datas_simulacao[mes],
            "Patrimônio Inicial Mês": patrimonio_inicial_mes_corrente,
            "Rendimento Mensal": rendimento_bruto_mes,
            "Imposto Pago Mensal": imposto_do_mes,
            "Aporte Mensal Ajustado": aporte_ajustado_mes_corrente,
            "Resgate Mensal Ajustado": resgate_efetivo_mes,
            "Patrimônio Final Mês": patrimonio_final_mes_corrente,
            "Rendimento Acumulado": rendimento_total,
            "Imposto Pago Acumulado": imposto_pago_acumulado,
            "Resgate Acumulado": resgate_total,
            "Aporte Acumulado": aporte_total,
            "Fator Inflação": fator_inflacao,
            "Inflação Acumulada": (fator_inflacao - 1) * 100
        })
    
    # Converter para DataFrame
    df_resultados = pd.DataFrame(resultados)
    
    # Inclui a idade do usuário no DataFrame e as colunas necessárias para as tabelas/gráficos
    df_resultados['Idade Contínua'] = (df_resultados['Data'] - data_nascimento).dt.days / 365.25
    df_resultados['Idade Anos'] = df_resultados['Idade Contínua'].apply(np.floor).astype(int)
    df_resultados['Idade Meses'] = ((df_resultados['Idade Contínua'] - df_resultados['Idade Anos']) * 12).apply(np.floor).astype(int)
    df_resultados['Idade Completa'] = df_resultados.apply(lambda x: f"{x['Idade Anos']} anos e {x['Idade Meses']} meses", axis=1)
    df_resultados['Idade Contínua'] = df_resultados['Idade Contínua'].round(2)

    # Reordenar colunas
    df_resultados = df_resultados[['Data', 'Idade Anos', 'Idade Meses', 'Idade Completa', 'Idade Contínua', 'Patrimônio Inicial Mês', 'Rendimento Mensal', "Imposto Pago Mensal", 'Aporte Mensal Ajustado', 'Resgate Mensal Ajustado', 'Patrimônio Final Mês', 'Rendimento Acumulado', "Imposto Pago Acumulado", 'Resgate Acumulado', 'Aporte Acumulado', 'Fator Inflação', 'Inflação Acumulada']]

    return df_resultados

def goal_seek(
    valor_target: float,
    variavel_target: Literal[
        "patrimonio_inicial", 
        "aporte_mensal", 
        "rentabilidade_nominal_esperada", 
        "resgate_mensal"
    ],
    parametros_base: Dict[str, Any],
    limite_inferior: float,
    limite_superior: float,
    tol: float = 1.0,
    max_iteracoes: int = 100
) -> float:
    """
    Encontra o valor de uma variável para atingir um objetivo de patrimônio final.

    Utiliza o método da bisseção para encontrar o valor de `variavel_target`
    que resulta em um `Patrimônio Final Mês` igual a `valor_target` na simulação.

    Args:
        valor_target (float): O valor alvo para o patrimônio final.
        variavel_target (str): O nome do parâmetro a ser ajustado.
        parametros_base (Dict[str, Any]): Dicionário com os parâmetros base da simulação.
        limite_inferior (float): Limite inferior para a busca da variável.
        limite_superior (float): Limite superior para a busca da variável.
        tol (float, optional): Tolerância para a diferença entre o valor encontrado
            e o alvo. Defaults to 1.0.
        max_iteracoes (int, optional): Número máximo de iterações. Defaults to 100.

    Returns:
        float: O valor da `variavel_target` que atinge o `valor_target`.

    Raises:
        ValueError: Se o alvo não for alcançável dentro dos limites fornecidos.
    """
    inferior = limite_inferior
    superior = limite_superior

    def obter_patrimonio_final(valor: float) -> float:
        parametros = parametros_base.copy()
        parametros[variavel_target] = valor
        df_sim = simular_patrimonio(**parametros)
        return df_sim.iloc[-1]["Patrimônio Final Mês"]

    valor_inferior = obter_patrimonio_final(inferior)
    valor_superior = obter_patrimonio_final(superior)

    if (valor_inferior - valor_target) * (valor_superior - valor_target) > 0:
        raise ValueError("Alvo fora do intervalo alcançável com os limites fornecidos.")

    for _ in range(max_iteracoes):
        meio = (inferior + superior) / 2
        valor_meio = obter_patrimonio_final(meio)

        if abs(valor_meio - valor_target) < tol:
            return meio

        if (valor_meio - valor_target) * (valor_inferior - valor_target) < 0:
            superior = meio
            valor_superior = valor_meio
        else:
            inferior = meio
            valor_inferior = valor_meio
    
    return (inferior + superior) / 2 