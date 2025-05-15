from visual_test_data import Visualiser
from analyze_test_data import DataAnalyzer

def main():
    """Инициализация компонентов"""
    analyzer = DataAnalyzer()
    visualizer = Visualiser()
    
    try:
        # Загрузка данных
        df = analyzer.load_data()
        
        if df.empty:
            print("База данных пуста!")
            return
        
        # Анализ данных
        basic_stats = analyzer.get_basic_stats(df)
        daily_stats = analyzer.get_daily_stats(df)
        
        print("Основная статистика:")
        print(f"Всего записей: {basic_stats['total_entries']}")
        print(f"Средний размер статьи: {basic_stats['average_char_count']:.0f} символов")
        print(f"Общие просмотры: {basic_stats['total_views']}")
        print(f"Последнее обновление: {basic_stats['last_update']}")
        
        # Сохранение статистики в файл
        analyzer.save_stats_to_txt(basic_stats, daily_stats)
        print("Статистика сохранена в stats_report.txt")
        
        # Визуализация
        figures = [
            visualizer.plot_content_evolution(df),
            visualizer.plot_views(daily_stats)
        ]
        visualizer.save_plots(figures)
        print("Графики сохранены")
        
    except Exception as e:
        print(f"Ошибка: {e}")
        
        
if __name__ == "__main__":
    main()
        