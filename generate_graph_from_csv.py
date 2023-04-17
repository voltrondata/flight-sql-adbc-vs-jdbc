import pandas as pd
import seaborn as sns
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent.resolve()
GRAPH_OUTPUT_DIR = SCRIPT_DIR / "graph_output"


def main():
    df = pd.read_csv(filepath_or_buffer=GRAPH_OUTPUT_DIR / "minute_bars.csv")
    ax = sns.kdeplot(data=df, fill=True)
    ax.get_legend().set_frame_on(False)
    ax.figure.set_size_inches(12, 3)
    ax.xaxis.set_label_text("seconds")
    ax.yaxis.set_visible(False)
    sns.despine(left=True)

    ax.figure.tight_layout()
    ax.figure.savefig(GRAPH_OUTPUT_DIR / "minute_bars_manual.svg")  # , transparent=True)


if __name__ == "__main__":
    main()
