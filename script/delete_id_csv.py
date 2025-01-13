import pandas as pd
import chardet
def convert_to_utf8(input_file, output_file):
    try:
        with open(input_file, 'r', encoding='utf-16') as f:
            content = f.read()

        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(content)

        print(f"成功将文件从 UTF-16 转换为 UTF-8，保存为: {output_file}")
    except Exception as e:
        print(f"发生错误: {e}")


def remove_id_column(input_file, output_file):
    try:
        # 读取 CSV 文件
        df = pd.read_csv(input_file, sep='\t')

        # 检查是否存在 'id' 列
        if 'id' in df.columns:
            # 删除 'id' 列
            df = df.drop(columns=['id'])
            print("成功删除 'id' 列。")
        else:
            print("'id' 列不存在。")

        # 输出到新的 CSV 文件
        df.to_csv(output_file, index=False, sep='\t')
        print(f"文件已保存到: {output_file}")

    except Exception as e:
        print(f"发生错误: {e}")


# 使用示例
input_csv = r"F:\Master\TRS\JFJ_spider\data\jingdong.csv"  # 替换为你的输入文件路径
output_csv = "output_1.tsv"  # 替换为你的输出文件路径
#convert_to_utf8(input_csv,'temp_utf8.tsv')

remove_id_column(input_csv, output_csv)
