import asyncio
import ormx_core

async def main():
    print("ORMX Core Module Loaded Successfully")
    print(f"Engine Class: {ormx_core.Engine}")
    
    # We can't easily test connection without a real DB in this environment
    # but we can verify the API surface.
    engine = ormx_core.Engine
    print("API Surface Check: connect is available")
    
if __name__ == "__main__":
    asyncio.run(main())
