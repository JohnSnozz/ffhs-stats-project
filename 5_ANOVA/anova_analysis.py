"""
ANOVA Analysis for Swiss Federal Voting Data
=============================================
Analyzes voting patterns across:
1. Sprachgebiete (Röstigraben)
2. Städtische/Ländliche Gebiete (Stadt-Land)
3. Grossregionen der Schweiz (Regional)

For all 223 proposals (2000-2025)
"""

import sqlite3
import pandas as pd
import numpy as np
from scipy import stats
import warnings
warnings.filterwarnings('ignore')

# Database connection
DB_PATH = '/home/jonas/ffhs-stats-project-local/data/processed/swiss_votings.db'

def load_data():
    """Load voting data with municipality features"""
    conn = sqlite3.connect(DB_PATH)

    query = """
    SELECT
        v.municipality_id,
        v.municipality_name,
        v.voting_date,
        v.proposal_id,
        v.title_de,
        v.ja_prozent,
        v.stimmbeteiligung,
        mf.sprachgebiete,
        mf.staedtische_laendliche_gebiete,
        mf.grossregionen_der_schweiz
    FROM v_voting_results_analysis v
    INNER JOIN municipality_features mf ON v.municipality_id = mf.bfs_nr
    WHERE v.ja_prozent IS NOT NULL
    """

    df = pd.read_sql_query(query, conn)

    # Load labels
    labels_query = """
    SELECT feature_name, code, label
    FROM feature_labels
    WHERE feature_name IN ('sprachgebiete', 'staedtische_laendliche_gebiete', 'grossregionen_der_schweiz')
    """
    labels_df = pd.read_sql_query(labels_query, conn)
    conn.close()

    # Create label mappings
    label_maps = {}
    for feature in ['sprachgebiete', 'staedtische_laendliche_gebiete', 'grossregionen_der_schweiz']:
        feature_labels = labels_df[labels_df['feature_name'] == feature]
        label_maps[feature] = dict(zip(feature_labels['code'], feature_labels['label']))

    return df, label_maps


def calculate_eta_squared(df, group_col, value_col):
    """Calculate eta-squared (effect size) for ANOVA"""
    groups = [group[value_col].values for name, group in df.groupby(group_col)]

    # Total sum of squares
    grand_mean = df[value_col].mean()
    ss_total = ((df[value_col] - grand_mean) ** 2).sum()

    # Between-group sum of squares
    ss_between = sum(len(g) * (g.mean() - grand_mean) ** 2 for g in groups)

    # Eta-squared
    eta_sq = ss_between / ss_total if ss_total > 0 else 0
    return eta_sq


def run_anova_for_proposal(df, proposal_id, group_col):
    """Run ANOVA for a single proposal and grouping variable"""
    proposal_data = df[df['proposal_id'] == proposal_id].copy()

    # Remove NaN values
    proposal_data = proposal_data.dropna(subset=[group_col, 'ja_prozent'])

    # Get groups
    groups = [group['ja_prozent'].values for name, group in proposal_data.groupby(group_col)]

    # Need at least 2 groups with data
    groups = [g for g in groups if len(g) > 0]
    if len(groups) < 2:
        return None

    # Run ANOVA
    try:
        f_stat, p_value = stats.f_oneway(*groups)

        # Calculate effect size (eta-squared)
        eta_sq = calculate_eta_squared(proposal_data, group_col, 'ja_prozent')

        # Calculate group means
        group_means = proposal_data.groupby(group_col)['ja_prozent'].agg(['mean', 'std', 'count'])

        # Get proposal info
        title = proposal_data['title_de'].iloc[0]
        voting_date = proposal_data['voting_date'].iloc[0]

        # Calculate range (max - min mean)
        mean_range = group_means['mean'].max() - group_means['mean'].min()

        return {
            'proposal_id': proposal_id,
            'title': title,
            'voting_date': voting_date,
            'f_statistic': f_stat,
            'p_value': p_value,
            'eta_squared': eta_sq,
            'mean_range': mean_range,
            'n_municipalities': len(proposal_data),
            'n_groups': len(groups),
            'group_means': group_means.to_dict()
        }
    except Exception as e:
        return None


def run_all_anova(df, group_col, group_name):
    """Run ANOVA for all proposals"""
    proposals = df['proposal_id'].unique()
    results = []

    print(f"\nRunning ANOVA for {group_name} ({len(proposals)} proposals)...")

    for i, proposal_id in enumerate(proposals):
        result = run_anova_for_proposal(df, proposal_id, group_col)
        if result:
            result['category'] = group_name
            results.append(result)

        if (i + 1) % 50 == 0:
            print(f"  Processed {i + 1}/{len(proposals)} proposals...")

    results_df = pd.DataFrame(results)

    # Add significance flag
    results_df['significant'] = results_df['p_value'] < 0.05
    results_df['highly_significant'] = results_df['p_value'] < 0.001

    return results_df


def format_group_means(group_means_dict, label_map):
    """Format group means for display"""
    means = group_means_dict['mean']
    formatted = []
    for code, mean in sorted(means.items(), key=lambda x: x[1], reverse=True):
        label = label_map.get(code, f"Code {code}")
        # Shorten long labels
        if len(label) > 25:
            label = label[:22] + "..."
        formatted.append(f"{label}: {mean:.1f}%")
    return " | ".join(formatted)


def main():
    print("=" * 80)
    print("ANOVA ANALYSIS - Swiss Federal Voting Data (2000-2025)")
    print("=" * 80)

    # Load data
    print("\nLoading data...")
    df, label_maps = load_data()
    print(f"Loaded {len(df):,} voting records")
    print(f"Unique proposals: {df['proposal_id'].nunique()}")
    print(f"Unique municipalities: {df['municipality_id'].nunique()}")

    # Define analyses
    analyses = [
        ('sprachgebiete', 'Röstigraben (Sprachgebiete)'),
        ('staedtische_laendliche_gebiete', 'Stadt-Land'),
        ('grossregionen_der_schweiz', 'Grossregionen'),
    ]

    all_results = []

    # Run ANOVA for each category
    for group_col, group_name in analyses:
        results_df = run_all_anova(df, group_col, group_name)
        all_results.append(results_df)

        # Summary statistics
        n_significant = results_df['significant'].sum()
        n_highly_sig = results_df['highly_significant'].sum()

        print(f"\n{group_name}:")
        print(f"  Total proposals analyzed: {len(results_df)}")
        print(f"  Significant (p < 0.05): {n_significant} ({100*n_significant/len(results_df):.1f}%)")
        print(f"  Highly significant (p < 0.001): {n_highly_sig} ({100*n_highly_sig/len(results_df):.1f}%)")
        print(f"  Mean eta-squared: {results_df['eta_squared'].mean():.4f}")

    # Combine all results
    combined_results = pd.concat(all_results, ignore_index=True)

    # Save full results
    output_path = '/home/jonas/ffhs-stats-project-local/5_ANOVA/anova_results_full.csv'
    combined_results.drop(columns=['group_means']).to_csv(output_path, index=False)
    print(f"\nFull results saved to: {output_path}")

    # Print top 15 per category
    print("\n" + "=" * 80)
    print("TOP 15 ABSTIMMUNGEN MIT GRÖSSTEN SIGNIFIKANTEN UNTERSCHIEDEN")
    print("=" * 80)

    for (group_col, group_name), results_df in zip(analyses, all_results):
        print(f"\n{'=' * 80}")
        print(f"  {group_name.upper()}")
        print(f"  (sortiert nach Effektstärke eta², nur signifikante p < 0.05)")
        print("=" * 80)

        # Filter significant and sort by effect size
        sig_results = results_df[results_df['significant']].sort_values('eta_squared', ascending=False)

        if len(sig_results) == 0:
            print("  Keine signifikanten Ergebnisse gefunden.")
            continue

        top_15 = sig_results.head(15)

        for idx, (_, row) in enumerate(top_15.iterrows(), 1):
            print(f"\n{idx:2}. {row['title'][:70]}...")
            print(f"    Datum: {row['voting_date']} | ID: {row['proposal_id']}")
            print(f"    F = {row['f_statistic']:.1f} | p = {row['p_value']:.2e} | eta² = {row['eta_squared']:.4f}")
            print(f"    Differenz (max-min): {row['mean_range']:.1f} Prozentpunkte")
            print(f"    {format_group_means(row['group_means'], label_maps[group_col])}")

    # Create summary table
    print("\n" + "=" * 80)
    print("ZUSAMMENFASSUNG")
    print("=" * 80)

    summary_data = []
    for (group_col, group_name), results_df in zip(analyses, all_results):
        sig_results = results_df[results_df['significant']].sort_values('eta_squared', ascending=False)

        summary_data.append({
            'Kategorie': group_name,
            'Anzahl Abstimmungen': len(results_df),
            'Signifikant (p<0.05)': results_df['significant'].sum(),
            'Anteil signifikant': f"{100*results_df['significant'].mean():.1f}%",
            'Mittlere Effektstärke (eta²)': f"{results_df['eta_squared'].mean():.4f}",
            'Max Effektstärke': f"{results_df['eta_squared'].max():.4f}",
            'Top Abstimmung': sig_results.iloc[0]['title'][:50] + "..." if len(sig_results) > 0 else "-"
        })

    summary_df = pd.DataFrame(summary_data)
    print(summary_df.to_string(index=False))

    # Save summary
    summary_path = '/home/jonas/ffhs-stats-project-local/5_ANOVA/anova_summary.csv'
    summary_df.to_csv(summary_path, index=False)
    print(f"\nSummary saved to: {summary_path}")

    return combined_results, all_results, analyses, label_maps


if __name__ == "__main__":
    combined_results, all_results, analyses, label_maps = main()
