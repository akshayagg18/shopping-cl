"""Official fixture-runner adapter for a static image, with sandbox mounts."""
import os,pathlib,subprocess,sys
image,data,out=sys.argv[1:4];data=pathlib.Path(data).resolve();out=pathlib.Path(out).resolve();out.chmod(0o777)
for name in ['user_daily','campaign_daily']:(out/name).mkdir(exist_ok=True);(out/name).chmod(0o777)
p=subprocess.run(['docker','run','--rm','--network','none','--read-only','--user','1000:1000','--tmpfs','/tmp:rw,size=64m','-e','DATA_DIR=/data','-e','OUTPUT_DIR=/output','-e','NCPU=8','-v',str(data)+':/data:ro','-v',str(out)+':/output',image]);raise SystemExit(p.returncode)
