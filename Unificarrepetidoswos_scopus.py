import pandas as pd
import spacy

try:
    # Cargar modelo de spaCy en español
    nlp = spacy.load('en_core_web_lg')
    
    # Función para preprocesar los títulos
    def preprocess_title(title):
        return title.replace(",", "").strip().lower()

    # Cargar los datos
    
    scopus_file_path = "G:\\Mi unidad\\2024\\Master Solis Granda Luis Eduardo\\data\\scopusfinaltitle.csv"
    
    wos_file_path = "G:\\Mi unidad\\2024\\Master Solis Granda Luis Eduardo\\data\\savedrecs1_1186.xls"
    try:
        # Leer los datos del archivo CSV de Scopus
        scopus_df = pd.read_csv(scopus_file_path)
        # Leer los datos del archivo Excel de Web of Science
        wos_df = pd.read_excel(wos_file_path)
        wos_df['Authors'] = wos_df['Authors'].str.replace(',', '')
    except Exception as e:
        print(f"Error al cargar los archivos: {e}")
        raise
 
    # Preprocesar los títulos en ambos dataframes
    scopus_df['processed_title'] = scopus_df['Title'].apply(preprocess_title)
    wos_df['processed_title'] = wos_df['Article Title'].apply(preprocess_title)

    # Contar los duplicados antes de eliminarlos
    scopus_duplicates_count = scopus_df.duplicated(subset=['processed_title']).sum()
    wos_duplicates_count = wos_df.duplicated(subset=['processed_title']).sum()

    print(f"Duplicados en Scopus antes de eliminar: {scopus_duplicates_count}")
    print(f"Duplicados en Web of Science antes de eliminar: {wos_duplicates_count}")

    # Eliminar duplicados dentro de cada dataframe basado en los títulos procesados
    scopus_df = scopus_df.drop_duplicates(subset=['processed_title'])
    wos_df = wos_df.drop_duplicates(subset=['processed_title'])
    #scopus_df = scopus_df.sample(n=6, random_state=1)
    # Comparar títulos
    def are_titles_similar(title1, title2, nlp, threshold=0.9):
        doc1 = nlp(title1)
        doc2 = nlp(title2)
        return doc1.similarity(doc2) > threshold
    # Validar y comparar los DOIs
    scopus_df['DOI'] = scopus_df['DOI'].fillna('').str.lower().str.strip()
    wos_df['DOI'] = wos_df['DOI'].fillna('').str.lower().str.strip()
    # Encontrar títulos similares
    similar_titles = []

    for index, wos_row in wos_df.iterrows():
        wos_title = wos_row['processed_title']
        wos_doi = wos_row['DOI']
        title_matched = False
        
        # Prioridad 1: Comparar títulos usando spaCy
        for scopus_title in scopus_df['processed_title']:
            if are_titles_similar(wos_title, scopus_title, nlp):
                similar_titles.append(wos_title)
                title_matched = True
                break
        
        # Prioridad 2: Comparar DOI si no hay coincidencia de título
        if not title_matched and wos_doi:
            if wos_doi in scopus_df['DOI'].values:
                similar_titles.append(wos_title)

  
    # Mostrar resultados
    print(f"En Web of Science hay {len(similar_titles)} títulos repetidos de Scopus.")
    print(f"En total hay {len(scopus_df) + len(wos_df)} títulos y {len(similar_titles)} títulos repetidos.")

    # Guardar los títulos repetidos en un archivo CSV
    output_file_path = "G:\\Mi unidad\\2024\\Master Solis Granda Luis Eduardo\\data\\wos_scopus_repeatedstitles.csv"
    repeated_titles_df = pd.DataFrame(similar_titles, columns=['Título Repetido'])
    
    try:
        repeated_titles_df.to_csv(output_file_path, index=False)
        print("Los títulos repetidos han sido guardados en 'repeated_titles.csv'.")
    except Exception as e:
        print(f"Error al guardar el archivo CSV: {e}")

 
    # Cambiar los nombres de las columnas en Web of Science para que coincidan con los de Scopus

    
    df_wos_renombrado = wos_df.rename(columns={
                'Authors': 'Authors',
                'Document Type': 'Document Type',
                'Language': 'Language of Original Document',
                'Author Keywords': 'Author Keywords',
                'Keywords Plus': 'Index Keywords',
                'Abstract': 'Abstract',
                'DOI': 'DOI',
                'Cited Reference Count': 'Cited by',
                'Publication Year': 'Year',
                'Source Title': 'Source title',
                'Article Title': 'Title',
                'Affiliations': 'Affiliationss',
                'Addresses': 'Affiliations',
                'ISSN': 'ISSN',
                'Publisher': 'Publisher',
                'DOI Link': 'Link',
                'Author Full Names': 'Author full names'
            })

     # Seleccionar solo las columnas necesarias en Web of Science
    necessary_columns = [
        'Authors', 'Document Type', 'Language of Original Document', 'Author Keywords', 
        'Index Keywords', 'Abstract', 'DOI', 'Cited by', 'Year', 'Source title', 
        'Title', 'Affiliations', 'ISSN', 'Publisher', 'Link', 'Author full names', 'processed_title'
    ]
    df_wos_renombrado = df_wos_renombrado[necessary_columns]
    # Filtrar dataframes originales para obtener los títulos no repetidos
    wos_non_repeated = df_wos_renombrado[~df_wos_renombrado['processed_title'].isin(similar_titles)]

    # Eliminar las columnas 'processed_title'
    scopus_df = scopus_df.drop(columns=['processed_title'])
    wos_non_repeated = wos_non_repeated.drop(columns=['processed_title'])    
    # Combinar datos de Web of Science en el DataFrame de Scopus
    combined_df = pd.concat([scopus_df, wos_non_repeated], ignore_index=True)
    # Guardar el DataFrame combinado en un archivo CSV
    combined_output_file_path = "G:\\Mi unidad\\2024\\Master Solis Granda Luis Eduardo\\data\\wos_scopus.csv"
    try:
        combined_df.to_csv(combined_output_file_path, index=False)
        print("Los datos combinados han sido guardados en 'combined_data.csv'.")
    
    except Exception as e:
     print(f"Error al guardar el archivo CSV combinado: {e}")


except Exception as e:
    print(f"Se produjo un error: {e}")
