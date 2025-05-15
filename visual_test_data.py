import matplotlib.pyplot as plt
import matplotlib

matplotlib.use("Agg")
from matplotlib.dates import DateFormatter
import pandas as pd


class Visualiser():
    def __init__(self):
        plt.style.use('ggplot')
        self.date_format = DateFormatter("%m-%d")

    def plot_content_evolution(self, df):
        """График изменения контента за последние 20 дней"""
        fig, ax = plt.subplots(figsize=(12, 6))

        # Сдвигаем график на один день назад (почему то идет на день вперед)
        shifted_dates = df['timestamp'] - pd.Timedelta(days=1)

        ax.plot(shifted_dates, df['char_count'], 'b-o')
        ax.set_title("Эволюция размера статьи за последние 20 дней")
        ax.set_ylabel("Количество символов")
        ax.xaxis.set_major_formatter(self.date_format)

        plt.close(fig)
        return fig

    def plot_views(self, daily_stats):
        """График просмотров за последние 20 дней"""
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))

        # Общие просмотры
        ax1.plot(daily_stats.index, daily_stats['total_views'], 'g--s')
        ax1.set_title("Общее количество просмотров за последние 20 дней")
        ax1.xaxis.set_major_formatter(self.date_format)

        # Среднедневные просмотры
        ax2.bar(daily_stats.index, daily_stats['average_daily_views'], color="orange")
        ax2.set_title("Средние просмотры в день")
        ax2.xaxis.set_major_formatter(self.date_format)
        ax2.set_ylim(2500, 4500)

        plt.tight_layout()
        plt.close(fig)
        return fig

    def save_plots(self, figures, prefix='plot'):
        """Сохранение графиков"""
        for i, fig in enumerate(figures, 1):
            fig.savefig(f'{prefix}_{i}.png')
            plt.close(fig)
            
            
    
        
        
