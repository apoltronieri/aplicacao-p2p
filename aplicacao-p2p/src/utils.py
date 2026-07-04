def mask_ip(ip: str) -> str:
    """
    Censura um endereço IP mantendo apenas o primeiro trecho.
    """
    parts = ip.split(".")

    if len(parts) != 4:
        return ip

    return f"{parts[0]}.***.**.**"