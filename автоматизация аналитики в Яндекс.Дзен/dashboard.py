import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

# подключениe к базе данных для Postresql
db_config = {'user': 'my_user',
             'pwd': 'my_user_password',
             'host': 'localhost',
             'port': 5432,
             'db': 'zen'}
engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(db_config['user'],
                                                            db_config['pwd'],
                                                            db_config['host'],
                                                            db_config['port'],
                                                            db_config['db']))
query = '''
            SELECT * FROM dash_engagement;
        '''
dash_engagement = pd.io.sql.read_sql(query, con = engine)
query = '''
            SELECT * FROM dash_visits;
        '''
dash_visits = pd.io.sql.read_sql(query, con = engine)

note = 'Здесь представлено три графика,' \
       'которые позволят проанализировать взаимодействие пользователей сервиса Яндекс.Дзен с карточками статей.' \
       'Фильтры позволяют отобрать интересующие возрастные категории, темы карточек, а также временной промежуток.'

dash_visits['dt'] = dash_visits['dt'].astype('datetime64[m]')
dash_engagement['dt'] = dash_engagement['dt'].astype('datetime64[m]')

# задаём лейаут
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.layout = html.Div(children=[

    # формируем html
    html.H3(children = 'Анализ пользовательского взаимодействия с карточками статей на Яндекс.Дзен'),
    html.Label(note),

    html.Div([
        html.Div([
            html.Label('Фильтр даты и времени',style={"font-weight": "bold"}),
            dcc.DatePickerRange(
                start_date = dash_visits['dt'].dt.date.min(),
                end_date = dash_visits['dt'].dt.date.max(),
                display_format = 'YYYY-MM-DD',
                id = 'dt_selector',
            )

        ], className = 'six columns'),

        html.Div([
            html.Label('Фильтр тем карточек', style={"font-weight": "bold"}),
            dcc.Dropdown(
                id = 'item-topic-dropdown',
                multi=True,
                options=[
                    {'label': item, 'value': item} for item in dash_visits['item_topic'].unique()
                ],
                value = dash_visits['item_topic'].unique().tolist()
            )
        ], className='six columns')
    ], className='row'),
    html.Div([
        html.Div([
            html.Label('Фильтр по возрастным группам', style={"font-weight": "bold"}),
            dcc.Dropdown(
                id = 'age-dropdown',
                multi=True,
                options=[
                    {'label': item, 'value': item} for item in dash_engagement['age_segment'].unique()
                ],
                value = dash_engagement['age_segment'].unique().tolist()
            )
        ],className='six columns')
    ], className='row'),

    html.Div([
        html.Div([
            html.Br(),
            html.Label('График истории событий по темам карточек', style={"font-weight": "bold"}),
            dcc.Graph(
                style={'height':'50vw'},
                id='history-absolute-visits'
            )
        ], className='six columns'),
        html.Div([
            html.Div([
                html.Br(),
                html.Label('График разбития событий по темам источников', style={"font-weight": "bold"}),
                dcc.Graph(
                    style={'height':'30vw'},
                    id='pie-visits'
                )
            ]),
            html.Div([
                html.Br(),
                html.Label('График средней глубины взаимодействия', style={"font-weight": "bold"}),
                dcc.Graph(
                    style={'height':'20vw'},
                    id='engagement-graph'
                )
            ])
        ], className='six columns')
    ],className='row')

])


# описываем логику дашборда
@app.callback(
    [Output('history-absolute-visits', 'figure'),
     Output('pie-visits', 'figure'),
     Output('engagement-graph', 'figure')
    ],
    [Input('item-topic-dropdown', 'value'),
     Input('age-dropdown', 'value'),
     Input('dt_selector', 'start_date'),
     Input('dt_selector', 'end_date')
    ])

def update_figures(selected_item_topics, selected_ages, start_date, end_date):
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')
    query = 'item_topic in @selected_item_topics and \
         dt >= @start_date and dt <= @end_date \
         and age_segment in @selected_ages'
    visits_filtered = dash_visits.query(query)
    engagement_filtered = dash_engagement.query(query)

    # формируем график истории событий по темам карточек
    data_visits = visits_filtered.groupby(['item_topic','dt']).agg({'visits':'sum'}).reset_index()
    visits_graph = []
    for topic in data_visits['item_topic'].unique():
        visits_graph += [go.Scatter(x=data_visits.query('item_topic==@topic')['dt'],
                                    y=data_visits.query('item_topic==@topic')['visits'],
                                    stackgroup='one',
                                    name = topic,
                                    mode='lines')]
    # формируем график разбития событий по темам источников
    pie_data = (visits_filtered.groupby('source_topic').agg({'visits':'sum'})
                .reset_index()
                .rename(columns={'visits':'counts'})
                )
    # формируем график средней глубины взаимодействия
    users_per_event = engagement_filtered.groupby('event').agg({'unique_users': 'mean'}).reset_index()
    users_per_event['conversion'] = users_per_event['unique_users'] / users_per_event.query('event=="show"')['unique_users'].sum()
    users_per_event = users_per_event.sort_values(by='conversion', ascending=False)
    return (
        {
            'data':visits_graph,
            'layout':go.Layout(xaxis = {'title': 'Время'},
                               yaxis = {'title': 'Число событий'})
        },
        {
            'data': [go.Pie(labels=pie_data['source_topic'],
                           values=pie_data['counts'])]
        },
        {
            'data': [go.Bar(x=users_per_event['event'],
                           y=users_per_event['conversion'],
                            text = round(users_per_event['conversion'],3),
                            textposition='auto')]
        }
    )

if __name__ == '__main__':
    app.run_server(debug = True, host='0.0.0.0')
