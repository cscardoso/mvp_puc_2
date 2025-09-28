# mvp_puc_2
## Descrição
Este trabalho apresenta um resumo do processo de treinamento de modelos utilizando dados meteorológicos fornecidos pelo INMET, abrangendo o período de 2018 a 2024. O objetivo é demonstrar a preparação, análise e aplicação dos dados para o desenvolvimento de soluções preditivas, destacando a importância da qualidade dos dados, das técnicas de processamento e dos métodos de validação empregados. O estudo enfatiza o potencial dos dados históricos do INMET para aprimorar previsões e apoiar tomadas de decisão em diferentes contextos meteorológicos.

## Estrutura do Projeto

- `mvp2.ipynb`: Notebook Jupyter com o desenvolvimento do projeto.
- `modelo_xgboost_precipitacao1.json`: Modelo 1 criado com xgboost
- `modelo_xgboost_precipitacao3.json`: Modelo 3 criado com xgboost
- `modelo_convlstm_precipitacao.keras`: Primeiro modelo gerado usando ConvLSTM
- `modelo_convlstm_precipitacao2.keras`: Segundo modelo gerado usando ConvLSTM com alteração de parâmetros, resultando em um modelo melhor na avaliação com novos dados.

## Como Executar

1. Clone este repositório.
2. Certifique-se de ter o Jupyter Notebook instalado.
3. Abra o arquivo `mvp2.ipynb` no Jupyter.
4. Execute as células para reproduzir a análise.

## Requisitos
- Python 3.10 ou superior
- Jupyter Notebook
- Bibliotecas: pandas, numpy, matplotlib, sklearn, tensorflow (ou outras mencionadas no notebook)

## Descrição do Problema
O problema abordado neste Jupyter Notebook foi a previsão de precipitação horária na região Sudeste do Brasil utilizando dados meteorológicos históricos do INMET (2018-2024). 
O objetivo foi construir modelos preditivos capazes de estimar a quantidade de chuva com base em variáveis como temperatura, pressão, umidade, radiação, entre outras.
Para isso, foi realizado tratamento dos dados, incluindo padronização de colunas, conversão de formatos, remoção de nulos e outliers, além da seleção de registros relevantes.
Foram testados diferentes algoritmos de regressão (RandomForest, XGBoost e ConvLSTM) e avaliados quanto à capacidade de generalização nos dados mais recentes (2024).
O projeto também discutiu desafios como desbalanceamento dos dados, limitações computacionais e a importância de variáveis ambientais adicionais para melhorar a precisão das previsões meteorológicas.

## Uso no Colab
Faça upload do arquivo mvp.ipynb no https://colab.research.google.com/ e execute.
O dataset será baixado direto do Kaggle e descompactado em uma das células do notebook.


