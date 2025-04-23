import os
import matplotlib.pyplot as plt
import polars as pl
import numpy as np
from statsmodels.stats.proportion import proportion_confint

def concat(df_a, df_b):
    try:
        return pl.concat([df_a, df_b], how='diagonal_relaxed')
    except:
        return pl.DataFrame(df_a.to_dicts() + df_b.to_dicts(), infer_schema_length=len(df_a) + len(df_b))

def calculate_confidence_interval(successes, total, confidence=0.95):
    """Calculate binomial confidence interval for proportion"""
    return proportion_confint(successes, total)

def main():
    df = pl.DataFrame()
    for filename in os.listdir('data'):
        if 'election_videos' in filename or 'hashtag_' in filename or '_videos' in filename:
            try:
                file_df = pl.read_parquet(f'./data/{filename}', columns=['createTime', 'id', 'aigcLabelType'])
                df = concat(df, file_df)
            except Exception as ex:
                print(f"File: {filename}, ex: {ex}")
    
    df = df.unique('id')
    df = df.with_columns(pl.from_epoch(pl.col('createTime').cast(pl.UInt64)))
    df = df.filter(pl.col('createTime').dt.year() >= 2025)
    
    # Group by week instead of month
    ai_df = df.sort('createTime')\
             .group_by_dynamic('createTime', every='2w')\
             .agg([
                pl.col('id').len().alias('total'),
                pl.col('id').filter(pl.col('aigcLabelType') == '2').len().alias('tiktok_tagged_ai'),
                pl.col('id').filter(pl.col('aigcLabelType') == '1').len().alias('user_tagged_ai')
             ])
    # ai_df = ai_df.head(len(ai_df) - 1)
    
    # Calculate percentages
    ai_df = ai_df.with_columns([
        (pl.col('tiktok_tagged_ai') / pl.col('total') * 100).alias('tiktok_ai_percent'),
        (pl.col('user_tagged_ai') / pl.col('total') * 100).alias('user_ai_percent')
    ])
    
    # Prepare lists for confidence intervals
    tiktok_ci_lower = []
    tiktok_ci_upper = []
    user_ci_lower = []
    user_ci_upper = []
    
    # Calculate confidence intervals
    for row in ai_df.iter_rows(named=True):
        # TikTok tagged AI
        t_lower, t_upper = calculate_confidence_interval(row['tiktok_tagged_ai'], row['total'])
        tiktok_ci_lower.append(t_lower * 100)  # Convert to percentage
        tiktok_ci_upper.append(t_upper * 100)
        
        # User tagged AI
        u_lower, u_upper = calculate_confidence_interval(row['user_tagged_ai'], row['total'])
        user_ci_lower.append(u_lower * 100)  # Convert to percentage
        user_ci_upper.append(u_upper * 100)
    
    # Add confidence intervals to the dataframe
    ai_df = ai_df.with_columns([
        pl.Series(name='tiktok_ci_lower', values=tiktok_ci_lower, dtype=pl.Float64),
        pl.Series(name='tiktok_ci_upper', values=tiktok_ci_upper, dtype=pl.Float64),
        pl.Series(name='user_ci_lower', values=user_ci_lower, dtype=pl.Float64),
        pl.Series(name='user_ci_upper', values=user_ci_upper, dtype=pl.Float64)
    ])
    
    # Plotting
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), sharex=True, gridspec_kw={'height_ratios': [1, 2]})
    
    # Top plot: Total videos count
    ax1.plot(ai_df['createTime'], ai_df['total'], color='blue', linewidth=2)
    ax1.set_ylabel('Total videos', color='blue')
    ax1.tick_params(axis='y', labelcolor='blue')
    ax1.set_title('Fortnightly Video Count')
    ax1.grid(True, alpha=0.3)
    
    # Bottom plot: AI percentages with confidence intervals
    dates = ai_df['createTime'].to_numpy()
    
    # Plot TikTok tagged AI percentage
    ax2.plot(dates, ai_df['tiktok_ai_percent'], color='red', linewidth=2, label='TikTok tagged AI')
    
    # Add shaded confidence interval for TikTok AI
    ax2.fill_between(dates, 
                     ai_df['tiktok_ci_lower'], 
                     ai_df['tiktok_ci_upper'], 
                     color='red', alpha=0.2)
    
    # Plot User tagged AI percentage
    ax2.plot(dates, ai_df['user_ai_percent'], color='green', linewidth=2, label='User tagged AI')
    
    # Add shaded confidence interval for User AI
    ax2.fill_between(dates, 
                     ai_df['user_ci_lower'], 
                     ai_df['user_ci_upper'], 
                     color='green', alpha=0.2)
    
    ax2.set_xlabel('Date')
    ax2.set_ylabel('Percentage of videos (%)')
    ax2.set_title('Percentage of AI-Generated Content Over Time')
    ax2.grid(True, alpha=0.3)
    ax2.legend(loc='upper left')
    
    plt.tight_layout()
    plt.savefig('ai_timeline_percentage.png', dpi=300)

if __name__ == '__main__':
    main()