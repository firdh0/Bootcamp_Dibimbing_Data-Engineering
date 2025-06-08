def analyze_top_subspecializations_by_source(combined_df, df_name="Combined News Data"):
    required_cols = ['subspecialization', 'news_source']
    if not combined_df.empty and all(col in combined_df.columns for col in required_cols):
        try:
            processed_df = combined_df.copy()
            processed_df = processed_df[
                (processed_df['subspecialization'] != "tidak dapat disimpulkan") &
                (pd.notna(processed_df['subspecialization']))
            ]
            if processed_df.empty:
                print(f"Tidak ada data subspesialisasi yang valid setelah filtering di {df_name}.")
                return

            unique_news_sources = sorted([
                source for source in processed_df['news_source'].unique()
                if pd.notna(source)
            ])

            if not unique_news_sources:
                print(f"Tidak ada sumber berita yang valid untuk dianalisis di {df_name}.")
                return

            fig = go.Figure()
            sources_with_traces = []

            for source_idx, current_news_source in enumerate(unique_news_sources):
                df_focused_on_source = processed_df[
                    processed_df['news_source'] == current_news_source
                ]

                if df_focused_on_source.empty:
                    print(f"Tidak ada data subspesialisasi untuk sumber: {current_news_source}")
                    continue

                subspecialization_counts = df_focused_on_source['subspecialization'].value_counts().reset_index()
                subspecialization_counts.columns = ['subspesialisasi', 'jumlah_sebutan']
                
                top_10_specs = subspecialization_counts.head(10) 

                if top_10_specs.empty:
                    print(f"Tidak ada data top 10 subspesialisasi untuk sumber: {current_news_source}")
                    continue
                
                sources_with_traces.append(current_news_source)
                fig.add_trace(go.Bar(
                    x=top_10_specs['subspesialisasi'], 
                    y=top_10_specs['jumlah_sebutan'], 
                    name=current_news_source,
                    visible=(len(sources_with_traces) == 1),
                    text=top_10_specs['jumlah_sebutan'],
                    textposition='auto',
                ))
            
            if not sources_with_traces:
                print("Tidak ada data untuk ditampilkan pada grafik setelah memproses semua sumber.")
                return

            buttons = []
            for i, news_source_name in enumerate(sources_with_traces):
                visibility_list = [False] * len(sources_with_traces)
                visibility_list[i] = True

                buttons.append(dict(
                    method='update',
                    label=news_source_name,
                    args=[{'visible': visibility_list},
                          {'title': f'Top 10 subspesialisasi dari Sumber: {news_source_name}',
                           'yaxis.title': 'Jumlah Sebutan',
                           'xaxis.title': 'subspesialisasi'
                           }]
                ))
            
            default_news_source = sources_with_traces[0]
            default_title = f'Top 10 subspesialisasi dari Sumber: {default_news_source}'

            fig.update_layout(
                title_text=default_title,
                title_x=0.5,
                xaxis_title="subspesialisasi",
                yaxis_title="Jumlah Sebutan",
                legend_title_text='Sumber Berita',
                updatemenus=[dict(
                    active=0,
                    buttons=buttons,
                    direction="down",
                    pad={"r": 10, "t": 10},
                    showactive=True,
                    x=0.01,
                    xanchor="left",
                    y=1.15,
                    yanchor="top"
                )],
                height=600,
                xaxis_tickangle=-45 
            )

            try:
                fig.show()
            except Exception as e:
                print(f"Tidak dapat menampilkan grafik untuk {df_name} secara langsung: {e}")
                print("Anda dapat menjalankan kode ini di lingkungan seperti Jupyter Notebook untuk melihat grafiknya.")

        except Exception as e:
            print(f"Terjadi kesalahan saat memproses data {df_name}: {e}")
            import traceback
            traceback.print_exc()
    else:
        if combined_df.empty:
            print(f"DataFrame {df_name} kosong. Analisis top 10 subspesialisasi dilewati.")
        else:
            missing_cols_list = [col for col in required_cols if col not in combined_df.columns]
            if missing_cols_list:
                print(f"DataFrame {df_name}: Kolom berikut tidak ditemukan: {', '.join(missing_cols_list)}. Analisis dilewati.")
