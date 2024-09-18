import csv
from datetime import datetime


def calculate_base_ranking(receptions, yards, touchdowns):
    return (yards * 0.1) + (receptions * 1) + (touchdowns * 6)


def safe_int_convert(value, default=0):
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def process_csv(file_path):
    player_rankings = []

    with open(file_path, 'r') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if len(row) < 5:  # Skip rows that don't have enough columns
                continue

            initial_ranking = safe_int_convert(row[0])
            name = row[1]
            receptions = safe_int_convert(row[2])
            yards = safe_int_convert(row[3])
            touchdowns = safe_int_convert(row[4])

            base_ranking = calculate_base_ranking(receptions, yards, touchdowns)

            player_rankings.append({
                'initial_ranking': initial_ranking,
                'name': name,
                'receptions': receptions,
                'yards': yards,
                'touchdowns': touchdowns,
                'base_ranking': base_ranking
            })

    # Sort players by their new base ranking, descending order
    player_rankings.sort(key=lambda x: x['base_ranking'], reverse=True)

    return player_rankings


def write_rankings_to_file(rankings, output_file):
    with open(output_file, 'w') as f:
        f.write(f"Football Player Rankings\n")
        f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

        for i, player in enumerate(rankings, 1):
            f.write(f"{i}. {player['name']} (Original Rank: {player['initial_ranking']})\n")
            f.write(
                f"   Receptions: {player['receptions']}, Yards: {player['yards']}, Touchdowns: {player['touchdowns']}\n")
            f.write(f"   Base Ranking Score: {player['base_ranking']:.2f}\n\n")


def main():
    input_file = 'playerdatatemp/playerstatsweek1.csv'  # Replace with your actual input file path
    output_file = 'player_rankings.txt'

    rankings = process_csv(input_file)
    write_rankings_to_file(rankings, output_file)

    print(f"Rankings have been written to {output_file}")


if __name__ == "__main__":
    main()
