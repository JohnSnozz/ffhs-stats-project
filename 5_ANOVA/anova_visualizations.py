"""
ANOVA Visualizations for Swiss Federal Voting Data
===================================================
Creates publication-ready plots for the ANOVA analysis results.
"""

import sqlite3
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')  # Headless backend
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
import warnings
warnings.filterwarnings('ignore')

# Set style
plt.style.use('seaborn-v0_8-whitegrid')
plt.rcParams['figure.figsize'] = (12, 8)
plt.rcParams['font.size'] = 11
plt.rcParams['axes.titlesize'] = 14
plt.rcParams['axes.labelsize'] = 12

DB_PATH = '/home/jonas/ffhs-stats-project-local/data/processed/swiss_votings.db'
OUTPUT_DIR = '/home/jonas/ffhs-stats-project-local/5_ANOVA'


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


def plot_effect_size_distribution():
    """Plot distribution of effect sizes (eta²) across all proposals"""
    results_df = pd.read_csv(f'{OUTPUT_DIR}/anova_results_full.csv')

    fig, axes = plt.subplots(1, 3, figsize=(15, 5))

    categories = [
        ('Röstigraben (Sprachgebiete)', 'tab:blue'),
        ('Stadt-Land', 'tab:orange'),
        ('Grossregionen', 'tab:green')
    ]

    for ax, (cat, color) in zip(axes, categories):
        data = results_df[results_df['category'] == cat]['eta_squared']
        ax.hist(data, bins=30, color=color, alpha=0.7, edgecolor='white')
        ax.axvline(data.mean(), color='red', linestyle='--', linewidth=2, label=f'Mittelwert: {data.mean():.3f}')
        ax.axvline(data.median(), color='darkred', linestyle=':', linewidth=2, label=f'Median: {data.median():.3f}')
        ax.set_xlabel('Effektstärke (eta²)')
        ax.set_ylabel('Anzahl Abstimmungen')
        ax.set_title(cat)
        ax.legend()

    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/effect_size_distribution.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved: effect_size_distribution.png")


def plot_top_proposals_comparison(df, label_maps):
    """Plot comparison of top proposals for each category"""
    results_df = pd.read_csv(f'{OUTPUT_DIR}/anova_results_full.csv')

    # Top 5 for each category
    top_proposals = {
        'Röstigraben': [166, 200, 103, 165, 157],  # From analysis output
        'Stadt-Land': [202, 185, 188, 172, 186],
        'Grossregionen': [222, 80, 106, 4, 200]
    }

    # Plot Röstigraben Top 5
    fig, axes = plt.subplots(2, 3, figsize=(16, 10))

    # Sprachgebiete labels
    sprach_labels = {1: 'DE', 2: 'FR', 3: 'IT', 4: 'RM'}
    sprach_colors = {'DE': '#FF6B6B', 'FR': '#4ECDC4', 'IT': '#45B7D1', 'RM': '#96CEB4'}

    for idx, proposal_id in enumerate(top_proposals['Röstigraben'][:6]):
        ax = axes[idx // 3, idx % 3]
        proposal_data = df[df['proposal_id'] == proposal_id]
        title = proposal_data['title_de'].iloc[0][:50] + "..."

        means = proposal_data.groupby('sprachgebiete')['ja_prozent'].mean()
        stds = proposal_data.groupby('sprachgebiete')['ja_prozent'].std()

        x_labels = [sprach_labels[k] for k in means.index]
        colors = [sprach_colors[l] for l in x_labels]

        bars = ax.bar(x_labels, means.values, yerr=stds.values, capsize=5, color=colors, alpha=0.8)
        ax.set_ylabel('Ja-Anteil (%)')
        ax.set_title(f'{title}', fontsize=10)
        ax.set_ylim(0, 100)

        # Add value labels
        for bar, mean in zip(bars, means.values):
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 3,
                   f'{mean:.1f}%', ha='center', va='bottom', fontsize=9)

    plt.suptitle('Top 6 Abstimmungen mit grösstem Röstigraben (Sprachgebiete)', fontsize=14, y=1.02)
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/top_roestigraben.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved: top_roestigraben.png")

    # Plot Stadt-Land Top 6
    fig, axes = plt.subplots(2, 3, figsize=(16, 10))

    stadt_labels = {1: 'Städtisch', 2: 'Intermediär', 3: 'Ländlich'}
    stadt_colors = {'Städtisch': '#E74C3C', 'Intermediär': '#F39C12', 'Ländlich': '#27AE60'}

    for idx, proposal_id in enumerate(top_proposals['Stadt-Land'][:6]):
        ax = axes[idx // 3, idx % 3]
        proposal_data = df[df['proposal_id'] == proposal_id]
        title = proposal_data['title_de'].iloc[0][:50] + "..."

        means = proposal_data.groupby('staedtische_laendliche_gebiete')['ja_prozent'].mean()
        stds = proposal_data.groupby('staedtische_laendliche_gebiete')['ja_prozent'].std()

        x_labels = [stadt_labels[k] for k in means.index]
        colors = [stadt_colors[l] for l in x_labels]

        bars = ax.bar(x_labels, means.values, yerr=stds.values, capsize=5, color=colors, alpha=0.8)
        ax.set_ylabel('Ja-Anteil (%)')
        ax.set_title(f'{title}', fontsize=10)
        ax.set_ylim(0, 100)

        for bar, mean in zip(bars, means.values):
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 3,
                   f'{mean:.1f}%', ha='center', va='bottom', fontsize=9)

    plt.suptitle('Top 6 Abstimmungen mit grösstem Stadt-Land Unterschied', fontsize=14, y=1.02)
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/top_stadt_land.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved: top_stadt_land.png")

    # Plot Grossregionen Top 3 (more complex)
    fig, axes = plt.subplots(1, 3, figsize=(18, 6))

    region_labels = {
        1: 'Léman', 2: 'Mittelland', 3: 'NW-CH',
        4: 'Zürich', 5: 'Ost-CH', 6: 'Zentral-CH', 7: 'Tessin'
    }
    region_colors = plt.cm.Set2(np.linspace(0, 1, 7))

    for idx, proposal_id in enumerate(top_proposals['Grossregionen'][:3]):
        ax = axes[idx]
        proposal_data = df[df['proposal_id'] == proposal_id]
        title = proposal_data['title_de'].iloc[0][:45] + "..."

        means = proposal_data.groupby('grossregionen_der_schweiz')['ja_prozent'].mean().sort_values(ascending=False)

        x_labels = [region_labels[k] for k in means.index]
        colors = [region_colors[k-1] for k in means.index]

        bars = ax.barh(x_labels, means.values, color=colors, alpha=0.8)
        ax.set_xlabel('Ja-Anteil (%)')
        ax.set_title(f'{title}', fontsize=10)
        ax.set_xlim(0, 100)

        for bar, mean in zip(bars, means.values):
            ax.text(mean + 1, bar.get_y() + bar.get_height()/2,
                   f'{mean:.1f}%', ha='left', va='center', fontsize=9)

    plt.suptitle('Top 3 Abstimmungen mit grösstem regionalen Unterschied', fontsize=14, y=1.02)
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/top_grossregionen.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved: top_grossregionen.png")


def plot_summary_heatmap():
    """Create summary visualization"""
    results_df = pd.read_csv(f'{OUTPUT_DIR}/anova_results_full.csv')

    # Create summary statistics
    summary = results_df.groupby('category').agg({
        'eta_squared': ['mean', 'median', 'max', 'std'],
        'significant': ['sum', 'mean'],
        'highly_significant': ['sum', 'mean']
    }).round(4)

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    # Effect size comparison
    ax = axes[0]
    categories = ['Röstigraben (Sprachgebiete)', 'Stadt-Land', 'Grossregionen']
    short_names = ['Röstigraben', 'Stadt-Land', 'Grossregionen']

    means = [results_df[results_df['category'] == c]['eta_squared'].mean() for c in categories]
    maxs = [results_df[results_df['category'] == c]['eta_squared'].max() for c in categories]

    x = np.arange(len(short_names))
    width = 0.35

    bars1 = ax.bar(x - width/2, means, width, label='Mittlere Effektstärke', color='steelblue', alpha=0.8)
    bars2 = ax.bar(x + width/2, maxs, width, label='Maximale Effektstärke', color='coral', alpha=0.8)

    ax.set_ylabel('Effektstärke (eta²)')
    ax.set_title('Vergleich der Effektstärken nach Kategorie')
    ax.set_xticks(x)
    ax.set_xticklabels(short_names)
    ax.legend()

    # Add value labels
    for bar in bars1:
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.01,
               f'{bar.get_height():.3f}', ha='center', va='bottom', fontsize=9)
    for bar in bars2:
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.01,
               f'{bar.get_height():.3f}', ha='center', va='bottom', fontsize=9)

    # Significance rates
    ax = axes[1]
    sig_rates = [results_df[results_df['category'] == c]['significant'].mean() * 100 for c in categories]
    high_sig_rates = [results_df[results_df['category'] == c]['highly_significant'].mean() * 100 for c in categories]

    bars1 = ax.bar(x - width/2, sig_rates, width, label='Signifikant (p<0.05)', color='#2E86AB', alpha=0.8)
    bars2 = ax.bar(x + width/2, high_sig_rates, width, label='Hochsignifikant (p<0.001)', color='#A23B72', alpha=0.8)

    ax.set_ylabel('Anteil Abstimmungen (%)')
    ax.set_title('Anteil signifikanter Ergebnisse nach Kategorie')
    ax.set_xticks(x)
    ax.set_xticklabels(short_names)
    ax.set_ylim(0, 110)
    ax.legend()

    for bar in bars1:
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
               f'{bar.get_height():.1f}%', ha='center', va='bottom', fontsize=9)
    for bar in bars2:
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
               f'{bar.get_height():.1f}%', ha='center', va='bottom', fontsize=9)

    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/anova_summary_comparison.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved: anova_summary_comparison.png")


def plot_detailed_example(df, label_maps):
    """Create detailed boxplot for one example proposal per category"""
    fig, axes = plt.subplots(1, 3, figsize=(16, 5))

    # Röstigraben example: Ernährungssouveränität (highest effect)
    ax = axes[0]
    proposal_data = df[df['proposal_id'] == 166].copy()
    proposal_data['Sprachgebiet'] = proposal_data['sprachgebiete'].map(label_maps['sprachgebiete'])
    order = ['Französisch', 'Italienisch', 'Deutsch', 'Rätoromanisch']
    sns.boxplot(data=proposal_data, x='Sprachgebiet', y='ja_prozent', order=order, ax=ax, palette='Set2')
    ax.set_title('Ernährungssouveränität (2018)\neta² = 0.78', fontsize=11)
    ax.set_ylabel('Ja-Anteil (%)')
    ax.set_xlabel('')
    ax.tick_params(axis='x', rotation=45)

    # Stadt-Land example: Massentierhaltung
    ax = axes[1]
    proposal_data = df[df['proposal_id'] == 202].copy()
    proposal_data['Region'] = proposal_data['staedtische_laendliche_gebiete'].map({
        1: 'Städtisch', 2: 'Intermediär', 3: 'Ländlich'
    })
    order = ['Städtisch', 'Intermediär', 'Ländlich']
    sns.boxplot(data=proposal_data, x='Region', y='ja_prozent', order=order, ax=ax, palette='Set1')
    ax.set_title('Massentierhaltungsinitiative (2022)\neta² = 0.24', fontsize=11)
    ax.set_ylabel('Ja-Anteil (%)')
    ax.set_xlabel('')

    # Grossregionen example: Liegenschaftssteuern
    ax = axes[2]
    proposal_data = df[df['proposal_id'] == 222].copy()
    proposal_data['Region'] = proposal_data['grossregionen_der_schweiz'].map({
        1: 'Léman', 2: 'Mittelland', 3: 'NW-CH',
        4: 'Zürich', 5: 'Ost-CH', 6: 'Zentral-CH', 7: 'Tessin'
    })
    means = proposal_data.groupby('Region')['ja_prozent'].mean().sort_values(ascending=False)
    order = means.index.tolist()
    sns.boxplot(data=proposal_data, x='Region', y='ja_prozent', order=order, ax=ax, palette='Set3')
    ax.set_title('Liegenschaftssteuern Zweitliegenschaften (2025)\neta² = 0.68', fontsize=11)
    ax.set_ylabel('Ja-Anteil (%)')
    ax.set_xlabel('')
    ax.tick_params(axis='x', rotation=45)

    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/anova_boxplot_examples.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved: anova_boxplot_examples.png")


def main():
    print("=" * 60)
    print("Creating ANOVA Visualizations")
    print("=" * 60)

    df, label_maps = load_data()

    plot_effect_size_distribution()
    plot_summary_heatmap()
    plot_top_proposals_comparison(df, label_maps)
    plot_detailed_example(df, label_maps)

    print("\nAll visualizations saved to:", OUTPUT_DIR)


if __name__ == "__main__":
    main()
