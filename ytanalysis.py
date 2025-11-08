"""
This is just for Testing, Do not use it in Production!

Part 2: YouTube数据情感分析器
从CSV或PostgreSQL导入数据，进行情感分析和数据可视化

安装依赖:
pip install pandas numpy textblob vaderSentiment matplotlib seaborn wordcloud psycopg2-binary scikit-learn
"""

import pandas as pd
import numpy as np
from textblob import TextBlob
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import matplotlib.pyplot as plt
import seaborn as sns
from wordcloud import WordCloud
import psycopg2
import re
from collections import Counter
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# 设置中文字体支持
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS', 'SimHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

class YouTubeSentimentAnalyzer:
    def __init__(self):
        """初始化情感分析器"""
        self.vader_analyzer = SentimentIntensityAnalyzer()
        self.df_channels = None
        self.df_videos = None
        self.df_comments = None
    
    def load_from_csv(self, data_dir='youtube_data'):
        """从CSV文件加载数据"""
        try:
            self.df_channels = pd.read_csv(f'{data_dir}/channel_data.csv')
            print(f"✓ 加载频道数据: {len(self.df_channels)} 条")
        except FileNotFoundError:
            print("⚠ 未找到频道数据文件")
        
        try:
            self.df_videos = pd.read_csv(f'{data_dir}/video_data.csv')
            print(f"✓ 加载视频数据: {len(self.df_videos)} 条")
        except FileNotFoundError:
            print("❌ 未找到视频数据文件")
            return False
        
        try:
            self.df_comments = pd.read_csv(f'{data_dir}/comment_data.csv')
            print(f"✓ 加载评论数据: {len(self.df_comments)} 条")
        except FileNotFoundError:
            print("⚠ 未找到评论数据文件")
        
        return True
    
    def load_from_postgres(self, db_config):
        """从PostgreSQL数据库加载数据"""
        try:
            conn = psycopg2.connect(**db_config)
            
            self.df_channels = pd.read_sql("SELECT * FROM channels", conn)
            print(f"✓ 加载频道数据: {len(self.df_channels)} 条")
            
            self.df_videos = pd.read_sql("SELECT * FROM videos", conn)
            print(f"✓ 加载视频数据: {len(self.df_videos)} 条")
            
            self.df_comments = pd.read_sql("SELECT * FROM comments", conn)
            print(f"✓ 加载评论数据: {len(self.df_comments)} 条")
            
            conn.close()
            return True
        except Exception as e:
            print(f"❌ 数据库加载错误: {e}")
            return False
    
    def clean_text(self, text):
        """清理文本数据"""
        if pd.isna(text) or text == '':
            return ''
        
        # 转换为字符串
        text = str(text)
        
        # 移除URL
        text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)
        
        # 移除HTML标签
        text = re.sub(r'<.*?>', '', text)
        
        # 移除特殊字符但保留基本标点
        text = re.sub(r'[^\w\s.,!?-]', '', text)
        
        # 移除多余空格
        text = ' '.join(text.split())
        
        return text
    
    def analyze_sentiment_textblob(self, text):
        """使用TextBlob进行情感分析"""
        if not text or text.strip() == '':
            return {
                'polarity': 0,
                'subjectivity': 0,
                'sentiment': 'neutral'
            }
        
        blob = TextBlob(text)
        polarity = blob.sentiment.polarity
        
        if polarity > 0.1:
            sentiment = 'positive'
        elif polarity < -0.1:
            sentiment = 'negative'
        else:
            sentiment = 'neutral'
        
        return {
            'polarity': polarity,
            'subjectivity': blob.sentiment.subjectivity,
            'sentiment': sentiment
        }
    
    def analyze_sentiment_vader(self, text):
        """使用VADER进行情感分析"""
        if not text or text.strip() == '':
            return {
                'compound': 0,
                'pos': 0,
                'neu': 1,
                'neg': 0,
                'sentiment': 'neutral'
            }
        
        scores = self.vader_analyzer.polarity_scores(text)
        
        # 根据compound score分类
        if scores['compound'] >= 0.05:
            sentiment = 'positive'
        elif scores['compound'] <= -0.05:
            sentiment = 'negative'
        else:
            sentiment = 'neutral'
        
        scores['sentiment'] = sentiment
        return scores
    
    def perform_sentiment_analysis(self):
        """对所有数据进行情感分析"""
        print("\n执行情感分析...")
        
        # 分析评论
        if self.df_comments is not None and len(self.df_comments) > 0:
            print("  分析评论情感...")
            self.df_comments['cleaned_text'] = self.df_comments['comment_text'].apply(self.clean_text)
            
            # TextBlob分析
            textblob_results = self.df_comments['cleaned_text'].apply(self.analyze_sentiment_textblob)
            self.df_comments['tb_polarity'] = textblob_results.apply(lambda x: x['polarity'])
            self.df_comments['tb_subjectivity'] = textblob_results.apply(lambda x: x['subjectivity'])
            self.df_comments['tb_sentiment'] = textblob_results.apply(lambda x: x['sentiment'])
            
            # VADER分析
            vader_results = self.df_comments['cleaned_text'].apply(self.analyze_sentiment_vader)
            self.df_comments['vader_compound'] = vader_results.apply(lambda x: x['compound'])
            self.df_comments['vader_pos'] = vader_results.apply(lambda x: x['pos'])
            self.df_comments['vader_neu'] = vader_results.apply(lambda x: x['neu'])
            self.df_comments['vader_neg'] = vader_results.apply(lambda x: x['neg'])
            self.df_comments['vader_sentiment'] = vader_results.apply(lambda x: x['sentiment'])
            
            print(f"  ✓ 完成 {len(self.df_comments)} 条评论的情感分析")
        
        # 分析视频标题和描述
        if self.df_videos is not None:
            print("  分析视频标题情感...")
            self.df_videos['title_cleaned'] = self.df_videos['title'].apply(self.clean_text)
            title_sentiment = self.df_videos['title_cleaned'].apply(self.analyze_sentiment_vader)
            self.df_videos['title_sentiment'] = title_sentiment.apply(lambda x: x['sentiment'])
            self.df_videos['title_compound'] = title_sentiment.apply(lambda x: x['compound'])
            
            print(f"  ✓ 完成 {len(self.df_videos)} 个视频标题的情感分析")
    
    def generate_statistics(self):
        """生成统计报告"""
        print("\n" + "=" * 60)
        print("数据统计报告")
        print("=" * 60)
        
        # 频道统计
        if self.df_channels is not None and len(self.df_channels) > 0:
            channel = self.df_channels.iloc[0]
            print(f"\n【频道信息】")
            print(f"名称: {channel['channel_name']}")
            print(f"订阅者: {channel['subscribers']:,}")
            print(f"总观看: {channel['total_views']:,}")
            print(f"总视频: {channel['total_videos']:,}")
        
        # 视频统计
        if self.df_videos is not None:
            print(f"\n【视频统计】")
            print(f"分析视频数: {len(self.df_videos)}")
            print(f"平均观看: {self.df_videos['view_count'].mean():,.0f}")
            print(f"平均点赞: {self.df_videos['like_count'].mean():,.0f}")
            print(f"平均评论: {self.df_videos['comment_count'].mean():,.0f}")
            print(f"总观看数: {self.df_videos['view_count'].sum():,}")
            print(f"总点赞数: {self.df_videos['like_count'].sum():,}")
            
            # 互动率
            self.df_videos['engagement_rate'] = (
                (self.df_videos['like_count'] + self.df_videos['comment_count']) / 
                self.df_videos['view_count'] * 100
            )
            print(f"平均互动率: {self.df_videos['engagement_rate'].mean():.2f}%")
        
        # 评论情感统计
        if self.df_comments is not None and 'vader_sentiment' in self.df_comments.columns:
            print(f"\n【评论情感分析 - VADER】")
            print(f"总评论数: {len(self.df_comments)}")
            
            sentiment_counts = self.df_comments['vader_sentiment'].value_counts()
            total = len(self.df_comments)
            
            for sentiment in ['positive', 'neutral', 'negative']:
                count = sentiment_counts.get(sentiment, 0)
                pct = (count / total * 100) if total > 0 else 0
                emoji = {'positive': '😊', 'neutral': '😐', 'negative': '😞'}
                print(f"{emoji[sentiment]} {sentiment.capitalize()}: {count:,} ({pct:.1f}%)")
            
            print(f"\n平均情感得分: {self.df_comments['vader_compound'].mean():.3f}")
            print(f"正面强度: {self.df_comments['vader_pos'].mean():.3f}")
            print(f"负面强度: {self.df_comments['vader_neg'].mean():.3f}")
        
        # 按视频的情感分布
        if self.df_comments is not None and 'vader_sentiment' in self.df_comments.columns:
            print(f"\n【视频情感排名】")
            video_sentiment = self.df_comments.groupby('video_id').agg({
                'vader_compound': 'mean',
                'comment_id': 'count'
            }).rename(columns={'comment_id': 'comment_count'})
            
            video_sentiment = video_sentiment.merge(
                self.df_videos[['video_id', 'title']], 
                on='video_id', 
                how='left'
            )
            
            print("\n最受欢迎的视频 (情感最积极):")
            top_positive = video_sentiment.nlargest(5, 'vader_compound')
            for idx, row in top_positive.iterrows():
                title = row['title'][:50] + '...' if len(row['title']) > 50 else row['title']
                print(f"  {row['vader_compound']:.3f} - {title} ({row['comment_count']}条评论)")
    
    def visualize_results(self, output_dir='analysis_results'):
        """生成可视化分析"""
        import os
        os.makedirs(output_dir, exist_ok=True)
        
        print(f"\n生成可视化分析...")
        
        # 1. 情感分析总览
        self._plot_sentiment_overview(output_dir)
        
        # 2. 视频性能分析
        self._plot_video_performance(output_dir)
        
        # 3. 时间序列分析
        self._plot_time_series(output_dir)
        
        # 4. 词云
        self._generate_wordclouds(output_dir)
        
        # 5. 详细情感分析
        self._plot_detailed_sentiment(output_dir)
        
        print(f"\n✓ 所有可视化结果已保存到 '{output_dir}' 目录")
    
    def _plot_sentiment_overview(self, output_dir):
        """情感分析总览"""
        if self.df_comments is None or 'vader_sentiment' not in self.df_comments.columns:
            return
        
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        
        # 1. 情感分布饼图
        sentiment_counts = self.df_comments['vader_sentiment'].value_counts()
        colors = {'positive': '#4CAF50', 'neutral': '#FFC107', 'negative': '#F44336'}
        axes[0, 0].pie(
            sentiment_counts.values, 
            labels=[f"{s.capitalize()}" for s in sentiment_counts.index],
            autopct='%1.1f%%',
            colors=[colors.get(s, '#999') for s in sentiment_counts.index],
            startangle=90
        )
        axes[0, 0].set_title('Comment Sentiment Distribution', fontsize=14, fontweight='bold')
        
        # 2. VADER compound分数分布
        axes[0, 1].hist(self.df_comments['vader_compound'], bins=50, color='skyblue', edgecolor='black', alpha=0.7)
        axes[0, 1].axvline(self.df_comments['vader_compound'].mean(), color='red', linestyle='--', linewidth=2, label='Mean')
        axes[0, 1].axvline(0, color='gray', linestyle=':', linewidth=1)
        axes[0, 1].set_xlabel('VADER Compound Score', fontsize=12)
        axes[0, 1].set_ylabel('Frequency', fontsize=12)
        axes[0, 1].set_title('Sentiment Score Distribution', fontsize=14, fontweight='bold')
        axes[0, 1].legend()
        
        # 3. 情感强度对比
        sentiment_intensities = pd.DataFrame({
            'Positive': self.df_comments.groupby('vader_sentiment')['vader_pos'].mean(),
            'Neutral': self.df_comments.groupby('vader_sentiment')['vader_neu'].mean(),
            'Negative': self.df_comments.groupby('vader_sentiment')['vader_neg'].mean()
        })
        sentiment_intensities.plot(kind='bar', ax=axes[1, 0], color=['#4CAF50', '#FFC107', '#F44336'])
        axes[1, 0].set_title('Sentiment Intensity by Category', fontsize=14, fontweight='bold')
        axes[1, 0].set_xlabel('Sentiment Category', fontsize=12)
        axes[1, 0].set_ylabel('Average Intensity', fontsize=12)
        axes[1, 0].set_xticklabels(axes[1, 0].get_xticklabels(), rotation=0)
        axes[1, 0].legend(title='Component')
        
        # 4. 评论长度 vs 情感
        self.df_comments['text_length'] = self.df_comments['cleaned_text'].str.len()
        for sentiment, color in colors.items():
            data = self.df_comments[self.df_comments['vader_sentiment'] == sentiment]
            axes[1, 1].scatter(data['text_length'], data['vader_compound'], 
                             alpha=0.3, s=20, c=color, label=sentiment.capitalize())
        axes[1, 1].set_xlabel('Comment Length (characters)', fontsize=12)
        axes[1, 1].set_ylabel('Sentiment Score', fontsize=12)
        axes[1, 1].set_title('Comment Length vs Sentiment', fontsize=14, fontweight='bold')
        axes[1, 1].legend()
        axes[1, 1].axhline(0, color='gray', linestyle=':', linewidth=1)
        
        plt.tight_layout()
        plt.savefig(f'{output_dir}/sentiment_overview.png', dpi=300, bbox_inches='tight')
        plt.close()
        print("  ✓ 情感分析总览")
    
    def _plot_video_performance(self, output_dir):
        """视频性能分析"""
        if self.df_videos is None:
            return
        
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        
        # 1. Top 10 观看量
        top_views = self.df_videos.nlargest(10, 'view_count')
        axes[0, 0].barh(range(len(top_views)), top_views['view_count'], color='steelblue')
        axes[0, 0].set_yticks(range(len(top_views)))
        axes[0, 0].set_yticklabels([t[:40]+'...' if len(t) > 40 else t for t in top_views['title']], fontsize=9)
        axes[0, 0].set_xlabel('View Count', fontsize=12)
        axes[0, 0].set_title('Top 10 Videos by Views', fontsize=14, fontweight='bold')
        axes[0, 0].invert_yaxis()
        
        # 2. 互动率 Top 10
        top_engagement = self.df_videos.nlargest(10, 'engagement_rate')
        axes[0, 1].barh(range(len(top_engagement)), top_engagement['engagement_rate'], color='coral')
        axes[0, 1].set_yticks(range(len(top_engagement)))
        axes[0, 1].set_yticklabels([t[:40]+'...' if len(t) > 40 else t for t in top_engagement['title']], fontsize=9)
        axes[0, 1].set_xlabel('Engagement Rate (%)', fontsize=12)
        axes[0, 1].set_title('Top 10 Videos by Engagement', fontsize=14, fontweight='bold')
        axes[0, 1].invert_yaxis()
        
        # 3. 观看 vs 点赞
        axes[1, 0].scatter(self.df_videos['view_count'], self.df_videos['like_count'], alpha=0.6, s=50)
        axes[1, 0].set_xlabel('View Count', fontsize=12)
        axes[1, 0].set_ylabel('Like Count', fontsize=12)
        axes[1, 0].set_title('Views vs Likes', fontsize=14, fontweight='bold')
        
        # 4. 点赞 vs 评论
        axes[1, 1].scatter(self.df_videos['like_count'], self.df_videos['comment_count'], alpha=0.6, s=50, color='green')
        axes[1, 1].set_xlabel('Like Count', fontsize=12)
        axes[1, 1].set_ylabel('Comment Count', fontsize=12)
        axes[1, 1].set_title('Likes vs Comments', fontsize=14, fontweight='bold')
        
        plt.tight_layout()
        plt.savefig(f'{output_dir}/video_performance.png', dpi=300, bbox_inches='tight')
        plt.close()
        print("  ✓ 视频性能分析")
    
    def _plot_time_series(self, output_dir):
        """时间序列分析"""
        if self.df_comments is None or 'published_at' not in self.df_comments.columns:
            return
        
        # 转换时间
        self.df_comments['published_date'] = pd.to_datetime(self.df_comments['published_at']).dt.date
        
        fig, axes = plt.subplots(2, 1, figsize=(15, 10))
        
        # 1. 每日评论数量
        daily_comments = self.df_comments.groupby('published_date').size()
        axes[0].plot(daily_comments.index, daily_comments.values, marker='o', linewidth=2)
        axes[0].set_xlabel('Date', fontsize=12)
        axes[0].set_ylabel('Number of Comments', fontsize=12)
        axes[0].set_title('Daily Comment Volume', fontsize=14, fontweight='bold')
        axes[0].grid(True, alpha=0.3)
        
        # 2. 每日平均情感
        daily_sentiment = self.df_comments.groupby('published_date')['vader_compound'].mean()
        axes[1].plot(daily_sentiment.index, daily_sentiment.values, marker='o', linewidth=2, color='purple')
        axes[1].axhline(0, color='gray', linestyle='--', linewidth=1)
        axes[1].fill_between(daily_sentiment.index, 0, daily_sentiment.values, 
                            where=(daily_sentiment.values > 0), alpha=0.3, color='green', label='Positive')
        axes[1].fill_between(daily_sentiment.index, 0, daily_sentiment.values, 
                            where=(daily_sentiment.values < 0), alpha=0.3, color='red', label='Negative')
        axes[1].set_xlabel('Date', fontsize=12)
        axes[1].set_ylabel('Average Sentiment Score', fontsize=12)
        axes[1].set_title('Daily Sentiment Trend', fontsize=14, fontweight='bold')
        axes[1].legend()
        axes[1].grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(f'{output_dir}/time_series.png', dpi=300, bbox_inches='tight')
        plt.close()
        print("  ✓ 时间序列分析")
    
    def _generate_wordclouds(self, output_dir):
        """生成词云"""
        if self.df_comments is None:
            return
        
        fig, axes = plt.subplots(1, 3, figsize=(18, 6))
        
        sentiments = ['positive', 'neutral', 'negative']
        colors = ['Greens', 'Greys', 'Reds']
        
        for idx, (sentiment, cmap) in enumerate(zip(sentiments, colors)):
            comments = self.df_comments[self.df_comments['vader_sentiment'] == sentiment]
            if len(comments) > 0:
                text = ' '.join(comments['cleaned_text'].astype(str))
                text = re.sub(r'\b\w{1,2}\b', '', text)  # 移除短词
                
                wordcloud = WordCloud(
                    width=600, height=400,
                    background_color='white',
                    colormap=cmap,
                    max_words=100
                ).generate(text)
                
                axes[idx].imshow(wordcloud, interpolation='bilinear')
                axes[idx].axis('off')
                axes[idx].set_title(f'{sentiment.capitalize()} Comments', fontsize=14, fontweight='bold')
        
        plt.tight_layout()
        plt.savefig(f'{output_dir}/wordclouds.png', dpi=300, bbox_inches='tight')
        plt.close()
        print("  ✓ 词云分析")
    
    def _plot_detailed_sentiment(self, output_dir):
        """详细情感分析"""
        if self.df_comments is None:
            return
        
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        
        # 1. TextBlob vs VADER对比
        axes[0, 0].scatter(self.df_comments['tb_polarity'], self.df_comments['vader_compound'], alpha=0.3, s=20)
        axes[0, 0].plot([-1, 1], [-1, 1], 'r--', linewidth=2, label='Perfect Agreement')
        axes[0, 0].set_xlabel('TextBlob Polarity', fontsize=12)
        axes[0, 0].set_ylabel('VADER Compound', fontsize=12)
        axes[0, 0].set_title('TextBlob vs VADER Comparison', fontsize=14, fontweight='bold')
        axes[0, 0].legend()
        axes[0, 0].grid(True, alpha=0.3)
        
        # 2. 主观性分析
        axes[0, 1].hist(self.df_comments['tb_subjectivity'], bins=30, color='orange', edgecolor='black', alpha=0.7)
        axes[0, 1].axvline(self.df_comments['tb_subjectivity'].mean(), color='red', linestyle='--', linewidth=2, label='Mean')
        axes[0, 1].set_xlabel('Subjectivity Score', fontsize=12)
        axes[0, 1].set_ylabel('Frequency', fontsize=12)
        axes[0, 1].set_title('Comment Subjectivity Distribution', fontsize=14, fontweight='bold')
        axes[0, 1].legend()
        
        # 3. 点赞数 vs 情感
        axes[1, 0].scatter(self.df_comments['like_count'], self.df_comments['vader_compound'], alpha=0.3, s=20)
        axes[1, 0].set_xlabel('Comment Likes', fontsize=12)
        axes[1, 0].set_ylabel('Sentiment Score', fontsize=12)
        axes[1, 0].set_title('Comment Popularity vs Sentiment', fontsize=14, fontweight='bold')
        axes[1, 0].axhline(0, color='gray', linestyle=':', linewidth=1)
        
        # 4. 情感分布箱线图
        sentiment_data = [
            self.df_comments[self.df_comments['vader_sentiment'] == 'positive']['vader_compound'],
            self.df_comments[self.df_comments['vader_sentiment'] == 'neutral']['vader_compound'],
            self.df_comments[self.df_comments['vader_sentiment'] == 'negative']['vader_compound']
        ]
        bp = axes[1, 1].boxplot(sentiment_data, labels=['Positive', 'Neutral', 'Negative'],
                                patch_artist=True)
        colors_box = ['#4CAF50', '#FFC107', '#F44336']
        for patch, color in zip(bp['boxes'], colors_box):
            patch.set_facecolor(color)
            patch.set_alpha(0.6)
        axes[1, 1].set_ylabel('VADER Compound Score', fontsize=12)
        axes[1, 1].set_title('Sentiment Score Distribution by Category', fontsize=14, fontweight='bold')
        axes[1, 1].grid(True, alpha=0.3, axis='y')
        
        plt.tight_layout()
        plt.savefig(f'{output_dir}/detailed_sentiment.png', dpi=300, bbox_inches='tight')
        plt.close()
        print("  ✓ 详细情感分析")
    
    def export_results(self, output_dir='analysis_results'):
        """导出分析结果"""
        import os
        os.makedirs(output_dir, exist_ok=True)
        
        if self.df_comments is not None:
            self.df_comments.to_csv(f'{output_dir}/comments_with_sentiment.csv', index=False, encoding='utf-8')
            print(f"✓ 评论情感分析结果: {output_dir}/comments_with_sentiment.csv")
        
        if self.df_videos is not None:
            self.df_videos.to_csv(f'{output_dir}/videos_with_analysis.csv', index=False, encoding='utf-8')
            print(f"✓ 视频分析结果: {output_dir}/videos_with_analysis.csv")


def main():
    """主函数"""
    print("=" * 60)
    print("YouTube 情感分析器 - Part 2")
    print("=" * 60)
    
    analyzer = YouTubeSentimentAnalyzer()
    
    # 选择数据源
    print("\n选择数据源:")
    print("1. CSV文件")
    print("2. PostgreSQL数据库")
    choice = input("请选择 (1/2): ").strip()
    
    success = False
    if choice == '1':
        data_dir = input("CSV数据目录 (默认: youtube_data): ").strip() or 'youtube_data'
        success = analyzer.load_from_csv(data_dir)
    elif choice == '2':
        print("\nPostgreSQL配置:")
        db_config = {
            'host': input("Host (默认: localhost): ").strip() or 'localhost',
            'database': input("Database (默认: youtube_db): ").strip() or 'youtube_db',
            'user': input("User (默认: postgres): ").strip() or 'postgres',
            'password': input("Password: ").strip() or 'password',
            'port': input("Port (默认: 5432): ").strip() or '5432'
        }
        success = analyzer.load_from_postgres(db_config)
    
    if not success:
        print("数据加载失败!")
        return
    
    # 执行分析
    analyzer.perform_sentiment_analysis()
    
    # 生成统计报告
    analyzer.generate_statistics()
    
    # 生成可视化
    output_dir = input("\n输出目录 (默认: analysis_results): ").strip() or 'analysis_results'
    analyzer.visualize_results(output_dir)
    
    # 导出结果
    analyzer.export_results(output_dir)
    
    print("\n" + "=" * 60)
    print("分析完成!")
    print("=" * 60)


if __name__ == "__main__":
    main()