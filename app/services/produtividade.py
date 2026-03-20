def calcular_score(servicos: int, suportes: int, infra: int, outros: int = 0) -> int:
    return (servicos * 3) + (suportes * 2) + (infra * 2) - (outros * 1)

def calcular_eficiencia(total: int, finalizadas: int) -> float:
    return round(finalizadas / total * 100, 1) if total > 0 else 0.0

def classificar_score(score: int) -> str:
    if score >= 20: return "excelente"
    if score >= 10: return "regular"
    return "critico"
