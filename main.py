# 终极路径修复 + 包导入
import sys
from pathlib import Path
# 强制把当前目录加入Python路径
sys.path.insert(0, str(Path(__file__).parent))

# 导入两种模式
from agents.chief_reviewer import ChiefReviewerAgent
from graph.workflow import run_workflow

def run_simple_mode(stock_code: str):
    """简单模式：只使用 ChiefReviewerAgent"""
    print("\n" + "="*80)
    print("[简单模式] ChiefReviewerAgent")
    print("="*80)
    print("\n首席审核员正在进行完整分析...")
    
    agent = ChiefReviewerAgent()
    result = agent.analyze(stock_code)
    
    print("\n" + "="*80)
    print(f"【{stock_code} 完整投研报告】")
    print("="*80)
    print("\n--- 最终整合报告 ---")
    print(result["final_report"])
    
    print("\n--- 各维度报告 ---")
    print("\n【资金面】")
    print(result["reports"]["capital"])
    print("\n【基本面】")
    print(result["reports"]["fundamental"])
    print("\n【行业面】")
    print(result["reports"]["industry"])
    print("\n【风险面】")
    print(result["reports"]["risk"])
    print("\n【技术面】")
    print(result["reports"]["technical"])
    if "valuation" in result["reports"]:
        print("\n【估值面】")
        print(result["reports"]["valuation"])
    
    return result

def main():
    print("="*80)
    print("A股5维机构级投研系统")
    print("="*80)
    
    print("\n请选择运行模式：")
    print("  1. 简单模式 - ChiefReviewerAgent（推荐，快速）")
    print("  2. 复杂模式 - LangGraph 工作流（完整流程）")
    
    choice = input("\n请输入选项 (1 或 2，默认1)：") or "1"
    
    stock_code = input("\n输入股票代码(默认600519)：") or "600519"
    
    try:
        if choice == "1":
            run_simple_mode(stock_code)
        elif choice == "2":
            run_workflow(stock_code)
        else:
            print("\n❌ 无效选项，默认使用简单模式")
            run_simple_mode(stock_code)
            
    except Exception as e:
        print(f"\n❌ 分析失败：{e}")
        import traceback
        print(traceback.format_exc())

    print("\n" + "="*80)
    print("✅ 分析完成！")
    print("="*80)

if __name__ == "__main__":
    main()