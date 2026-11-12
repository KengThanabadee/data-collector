"""Configurable public Bybit origins for the selected option transport."""


def public_origins(config):
    """Keep venue/environment identity independent of its public DNS alias."""
    rest=config.bybit_rest_host or ('api.bybit.com' if config.environment=='production' else 'api-testnet.bybit.com')
    websocket=config.bybit_ws_host or ('stream.bybit.com' if config.environment=='production' else 'stream-testnet.bybit.com')
    return 'https://'+rest,'wss://'+websocket+'/v5/public/option'
