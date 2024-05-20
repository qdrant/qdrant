import argparse
import json
import os
import re
from datetime import datetime

import dash
import plotly
from dash import dcc, html
from dash.dependencies import Input, Output, State
import pandas as pd
from plotly.subplots import make_subplots
import plotly.graph_objects as go

DATA_FOLDER = "tests_results"

parser = argparse.ArgumentParser(description='Run tests and calculate load time.')
parser.add_argument('--inputs-folder', nargs='+', type=str,
                    default=DATA_FOLDER, help='folder with files')
parser.add_argument('--inputs-format', type=str,
                    default='jsonl', help='')

args = parser.parse_args()


def list_data_files(input_folder, ending):
    result_list = []
    for filename in os.listdir(input_folder):
        if filename.endswith(ending) and os.path.isfile(os.path.join(input_folder, filename)):
            result_list.append(filename)
    return sorted(result_list)


def parse_log_file(file_path):
    timestamp_pattern = r'\b\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z\b'
    data = []
    with open(file_path, "r") as log_file:
        for line in log_file:
            if "SIGTERM received" in line or "systemd[1]: Stopping" in line:
                break
            # Only take those lines that starts with the timestamp
            match = re.search(timestamp_pattern, line)
            if match:
                line = line.replace("  ", " ")
                timestamp_str, log_lvl, _, event_info = line.strip().split(" ", 3)
                timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%fZ")
                if ": " in event_info:
                    event_source, event_msg = event_info.split(": ", 1)
                    data.append((timestamp, event_source.strip(), event_msg.strip()))

    df = pd.DataFrame(data, columns=['timestamp', 'event_source', 'event_msg'])
    earliest_timestamp = df['timestamp'].min()
    df['delay'] = (df['timestamp'] - earliest_timestamp).dt.total_seconds() * 1000
    df['duration'] = df.groupby(['event_source'])['timestamp'].diff().fillna(pd.Timedelta(seconds=0))

    start_events = df[df['event_msg'] == 'start']
    end_events = df[df['event_msg'] == 'end']

    durations = end_events.merge(start_events, on='event_source', suffixes=('_end', '_start'))
    durations['duration'] = (durations['timestamp_end'] - durations['timestamp_start']).dt.total_seconds() * 1000
    return durations


INPUT_FOLDER = args.inputs_folder
MRI_FORMAT = args.inputs_format
INPUT_FILES = list_data_files(INPUT_FOLDER, MRI_FORMAT)

app = dash.Dash(__name__)
app.layout = html.Div(children=[
        dcc.Store(id='dropdown-store', storage_type='session'),
        dcc.Dropdown(INPUT_FILES, INPUT_FILES[0], id='dropdown-selection'),
        dcc.Dropdown(INPUT_FILES, INPUT_FILES[1], id='dropdown-selection-2'),
        dcc.Graph(id='line-chart', config=dict(responsive=True)),
    ],
    style={'display': 'inline-block', 'width': '100%', 'height': '100%'}
)


@app.callback(
    Output('dropdown-store', 'data'),
    [
        Input('dropdown-selection', 'value'),
        Input('dropdown-selection-2', 'value')
    ]
)
def store_selected_values(value1, value2):
    return json.dumps({'value1': value1, 'value2': value2})


@app.callback(
    [Output('dropdown-selection', 'value'),
     Output('dropdown-selection-2', 'value')],
    [
        Input('dropdown-store', 'modified_timestamp')
    ],
    [State('dropdown-store', 'data')]
)
def restore_dropdown_values(timestamp, data):
    if timestamp is None:
        raise dash.exceptions.PreventUpdate

    stored_values = json.loads(data) if data else None
    value1 = stored_values['value1'] if stored_values else INPUT_FILES[0]
    value2 = stored_values['value2'] if stored_values else INPUT_FILES[1]

    return value1, value2


@app.callback(
    Output('line-chart', 'figure'),
    [
        Input('dropdown-selection', 'value'),
        Input('dropdown-selection-2', 'value'),
    ]
)
def update_graph(filename_1, filename_2):
    cols = plotly.colors.DEFAULT_PLOTLY_COLORS

    df_1 = pd.read_json(f'{DATA_FOLDER}/{filename_1}', lines=True)
    df_2 = pd.read_json(f'{DATA_FOLDER}/{filename_2}', lines=True)
    tmp_1 = filename_1.replace(f".{MRI_FORMAT}", '')
    tmp_2 = filename_2.replace(f".{MRI_FORMAT}", '')

    df_load_time = pd.read_csv(f'{DATA_FOLDER}/load_time.csv')
    df_load_time.set_index('item', inplace=True)

    metric_names = sorted([x for x in df_1.columns if x != 'delay'])
    subplot_titles = [x for x in metric_names]
    subplot_titles.append(tmp_1)
    subplot_titles.append(tmp_2)
    fig = make_subplots(rows=len(metric_names) + 2, cols=1,
                        shared_xaxes=True,
                        vertical_spacing=0.03, horizontal_spacing=0.009,
                        subplot_titles=subplot_titles)

    row_counter = 1
    for metric in metric_names:
        for item_zipped in [(filename_1, df_1, cols[0]), (filename_2, df_2, cols[1])]:
            fig.add_trace(go.Scatter(
                x=item_zipped[1]['delay'],
                y=item_zipped[1][metric],
                name=item_zipped[0],
                line=dict(width=2, color=item_zipped[2]),
                marker=dict(color=item_zipped[2]),
            ), row=row_counter, col=1)
        row_counter += 1

    text = f"load time, ms: {df_load_time.loc[tmp_1, 'load_time']} vs {df_load_time.loc[tmp_2, 'load_time']}"

    df_timeline_1 = parse_log_file(f'{DATA_FOLDER}/{tmp_1}.log')
    df_timeline_2 = parse_log_file(f'{DATA_FOLDER}/{tmp_2}.log')

    for df in [df_timeline_1, df_timeline_2]:
        for i, event_source in enumerate(df['event_source'].unique(), start=1):
            df_filtered = df[df['event_source'] == event_source]
            for index, row in df_filtered.iterrows():
                duration = row['duration']
                hover_text = f'{event_source}: {duration} ms'
                fig.add_trace(
                    go.Scatter(
                        # x=[row['timestamp_start'].timestamp(), row['timestamp_end'].timestamp()],
                        x=[row['delay_start'], row['delay_end']],
                        y=[i, i],
                        mode='lines',
                        name=event_source,
                        line=dict(width=6),
                        hoverinfo='text',
                        hovertext=hover_text,
                        hoveron='points+fills',
                    ),
                    row=row_counter, col=1
                )
        row_counter += 1

    fig.update_layout(height=1500, autosize=True, title_text=text)
    fig['layout']['xaxis']['showticklabels'] = True
    for i in range(1, len(metric_names) + 2):
        fig['layout'][f'xaxis{i}']['showticklabels'] = True
    return fig


if __name__ == '__main__':
    app.run_server(debug=True)
