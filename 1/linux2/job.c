#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <string.h>
#include <signal.h>
#include <fcntl.h>
#include <time.h>
#include "job.h"

//#define DEBUG

int jobid=0;
int siginfo=1;
int fifo;
int globalfd;
int i=0;

struct queue *Q=NULL,*head=NULL;

struct waitqueue *next=NULL;
struct waitqueue *current =NULL;

/* 调度程序 */
void scheduler()
{
	struct jobinfo *newjob=NULL;
	struct jobcmd cmd;
	int  count = 0;
	bzero(&cmd,DATALEN);
	if((count=read(fifo,&cmd,DATALEN))<0)
		error_sys("read fifo failed");
#ifdef DEBUG
	printf("Read whether other process send commend\n");
	if(count){
		printf("cmd cmdtype\t%d\ncmd defpri\t%d\ncmd data\t%s\n",cmd.type,cmd.defpri,cmd.data);
	}
	else
		printf("no data read\n");
#endif

	/* 更新等待队列中的作业 */
#ifdef DEBUG
	printf("update jobs in wait queue!\n");
#endif	
	updateall();
	
	switch(cmd.type){
	case ENQ:
		#ifdef DEBUG
			printf("Execute enq!\n");
		#endif
#ifdef DEBUG
	printf("before the do_ENQ!\n");
 	tell_before();
#endif
		do_enq(newjob,cmd);
#ifdef DEBUG
 	tell_after();
#endif
		break;
	case DEQ:
		#ifdef DEBUG
			printf("Execute deq!\n");
		#endif
#ifdef DEBUG
	printf("before the do_DEQ\n");
 	tell_before();
#endif
		do_deq(cmd);
#ifdef DEBUG
 	tell_after();
#endif
		break;
	case STAT:
		#ifdef DEBUG
			printf("Execute stat!\n");
		#endif
#ifdef DEBUG
	printf("before the do_STAT!\n");
 	tell_before();
#endif
		do_stat(cmd);
#ifdef DEBUG
 	tell_after();
#endif
		break;
	default:
		break;
	}
	/* 选择高优先级作业 */
 	if(current!=NULL&&i!=current->round) ;
	else{
		#ifdef DEBUG
			printf("Switch which job to run next!\n");
		#endif
		next=jobselect();
		printf("----------------------------\n");
		i=0;
		#ifdef DEBUG
			printf("Switch to the next job!\n");
		#endif
#ifdef DEBUG
	printf("before the job_switch!\n");
 	tell_before();
#endif
		jobswitch();
#ifdef DEBUG
 	tell_after();
#endif
	}
}

int allocjid()
{
	return ++jobid;
}
int add_round(int round)
{
	if(round==1)
		return 2;
	else if(round==2)
		return 5;
	else if(round==5)
		return 5;
	else  return-1;
}
void creat_Q()
{
	struct queue *tmp;
	int i=0;
 	if((tmp = (struct queue *)malloc(sizeof(struct queue)))==NULL)  
	{
		perror("malloc");  
		exit(1);  
	}
	tmp->wq=NULL;
	tmp ->prio = i+1;
	tmp->round=1;
	tmp->next=NULL;
	i++;  
	head=Q=tmp;
	if((tmp = (struct queue *)malloc(sizeof(struct queue)))==NULL)  
	{
		perror("malloc");  
		exit(1);  
	}
	tmp->wq=NULL;
	tmp ->prio = i+1;
	tmp->round=2;
	tmp->next=NULL;
	i++; 
	Q->next=tmp; 
	if((tmp = (struct queue *)malloc(sizeof(struct queue)))==NULL)  
	{
		perror("malloc");  
		exit(1);  
	}
	tmp->wq=NULL;
	tmp ->prio = i+1;
	tmp->round=5;
	tmp->next=NULL;
	i++;  
 	Q->next->next=tmp;
}

void tell_before()
{
	char timebuf[BUFLEN];
	struct queue *QQ;
	struct waitqueue *x;
 
	QQ=head;
	printf("JOBID\tPID\tOWNER\tRUNTIME\tWAITTIME\tCREATTIME\t\tSTATE\n");
	if(current){
		strcpy(timebuf,ctime(&(current->job->create_time)));
		timebuf[strlen(timebuf)-1]='\0';
		printf("%d\t%d\t%d\t%d\t%d\t%s\t%s\n",
			current->job->jid,
			current->job->pid,
			current->job->ownerid,
			current->job->run_time,
			current->job->wait_time,
			timebuf,"RUNNING");
	}
	for(;QQ!=NULL;QQ=QQ->next){
		for(x=QQ->wq;x!=NULL;x=x->next){
			strcpy(timebuf,ctime(&(x->job->create_time)));
			timebuf[strlen(timebuf)-1]='\0';
			printf("%d\t%d\t%d\t%d\t%d\t%s\t%s\n",
				x->job->jid,
				x->job->pid,
				x->job->ownerid,
				x->job->run_time,
				x->job->wait_time,
				timebuf,
				"READY");
		}
	}
 
}
void tell_after()
{
	char timebuf[BUFLEN];
	struct queue *QQ;
	struct waitqueue *x;
 
	QQ=head;
	//printf("JOBID\tPID\tOWNER\tRUNTIME\tWAITTIME\tCREATTIME\t\tSTATE\n");
	printf("after it!\n");
	if(current){
		strcpy(timebuf,ctime(&(current->job->create_time)));
		timebuf[strlen(timebuf)-1]='\0';
		printf("%d\t%d\t%d\t%d\t%d\t%s\t%s\n",
			current->job->jid,
			current->job->pid,
			current->job->ownerid,
			current->job->run_time,
			current->job->wait_time,
			timebuf,"RUNNING");
	}
	for(;QQ!=NULL;QQ=QQ->next){
		for(x=QQ->wq;x!=NULL;x=x->next){
			strcpy(timebuf,ctime(&(x->job->create_time)));
			timebuf[strlen(timebuf)-1]='\0';
			printf("%d\t%d\t%d\t%d\t%d\t%s\t%s\n",
				x->job->jid,
				x->job->pid,
				x->job->ownerid,
				x->job->run_time,
				x->job->wait_time,
				timebuf,
				"READY");
		}
	}
 
}
void updateall()
{
	struct queue *p,*s,*r;
	struct waitqueue *re,*f,*prev_f;
#ifdef DEBUG
	printf("before th updateall!\n");
 	tell_before();
#endif
	/* 更新作业运行时间 */
	if(current)
		current->job->run_time += 1; /* 加1代表1000ms */
	/* 更新作业等待时间及优先级 */
	for(r=p = head; p != NULL; p = p->next){
		for(prev_f=f=p->wq;f!=NULL;prev_f=f,f=f->next){
			f->job->wait_time += 1000;
			if(f->job->wait_time >= 10000&&p!=head){
				printf("one job upgrade!\n");
				f->job->wait_time=0;
				if(p->round==2){
					r=head;
					re=r->wq;
					if(prev_f==f)
						head->next->wq=head->next->wq->next;
					else
						prev_f->next=f->next;
					if(re!=NULL){
						for(;re->next!=NULL;re=re->next)
							;
						re->next=f;
					}
					else head->wq=f;
				}
				if(p->round==5){
					r=head->next;
					re=r->wq;
					if(prev_f==f)
						head->next->next->wq=head->next->next->wq->next;
					else
						prev_f->next=f->next;
					f->next=NULL;
					if(re!=NULL){
						for(;re->next!=NULL;re=re->next)
							;
						re->next=f;
					}
					else head->next->wq=f;
				}
			}
		}
		r=p;
	}
#ifdef DEBUG
	 tell_after();
#endif
}

struct waitqueue* jobselect()
{
	struct queue *p,*select;
	struct waitqueue *f=NULL,*selectprev;
	char timebuf[BUFLEN];
	select = head;
	selectprev = NULL;
		/* 遍历等待队列中的作业，找到优先级最高的作业 */
		for(p = head; p != NULL;p = p->next){
			if( (current==NULL&&p->wq!=NULL)||(p->wq!=NULL&&current!=NULL&&p->round<=current->round)){
				select = p;
				f=selectprev=p->wq;
				select->wq=f->next;
				f->round=select->round;
				//printf("select suce!!!!!!!!!!!!!!!!!!!!!!!!!\n");
				break;
			}
			else ;//printf("P==NULL\n");
		}
#ifdef DEBUG
	printf("the selected job!\n");
	printf("JOBID\tPID\tOWNER\tRUNTIME\tWAITTIME\tCREATTIME\t\tSTATE\n");
	if(f){
		strcpy(timebuf,ctime(&(f->job->create_time)));
		timebuf[strlen(timebuf)-1]='\0';
		printf("%d\t%d\t%d\t%d\t%d\t%s\t%s\n",
			f->job->jid,
			f->job->pid,
			f->job->ownerid,
			f->job->run_time,
			f->job->wait_time,
			timebuf,"SELECTED");
	}
#endif
	return f;
}

void jobswitch()
{
	struct waitqueue *p;
	int i;
	if(current && current->job->state == DONE){ /* 当前作业完成 */
		/* 作业完成，删除它 */
		for(i = 0;(current->job->cmdarg)[i] != NULL; i++){
			free((current->job->cmdarg)[i]);
			(current->job->cmdarg)[i] = NULL;
		}
		/* 释放空间 */
		free(current->job->cmdarg);
		free(current->job);
		free(current);
		//printf("current===NULL!!!!!!!!!\n");
		current = NULL;
	}
	
	if( next== NULL && current == NULL) /* 没有作业要运行 */{
		//printf("current===NULL\n");
		return;
	}
	else if (next!= NULL && current == NULL){ /* 开始新的作业 */
		printf("begin start new job\n");
		current =  next;
		next = NULL;
		current->job->state = RUNNING;
		//printf("continue the job--------:%d\n",current->job->pid);
		kill(current->job->pid,SIGCONT);
		return;
	}
	else if (next!= NULL && current != NULL){ /* 切换作业 */
#ifdef DEBUG
	printf("current job is:%d\n",current->job->jid);
	printf("next job is:%d\nthe struct queue is\n", next->job->jid);
	struct queue *k;
	char timebuf[BUFLEN];
	if(head!=NULL)
		printf("JOBID\tPID\tOWNER\tRUNTIME\tWAITTIME\tCREATTIME\t\tSTATE\n");
	else printf("NULL\n");
	for(k=head;k!=NULL;k=k->next){
		strcpy(timebuf,ctime(&(k->wq->job->create_time)));
		timebuf[strlen(timebuf)-1]='\0';
		printf("%d\t%d\t%d\t%d\t%d\t%s\t%s\n",
			k->wq->job->jid,
			k->wq->job->pid,
			k->wq->job->ownerid,
			k->wq->job->run_time,
			k->wq->job->wait_time,
			timebuf,
			"READY");
	}
#endif
		printf("switch to Pid: %d\n", next->job->pid);
		kill(current->job->pid,SIGSTOP);
		current->job->curpri = current->job->defpri;
		current->job->wait_time = 0;
		current->job->state = READY;
		current->next=NULL;

		/* 放回等待队列 */
		if(current->round==1){
			p = head->next->wq;
			if(p){
				for(; p ->next!= NULL; p = p->next);
				p->next= current;
			}
			else head->next->wq=current;
			//printf("fffffffffffffffff111\n");
		}else if(current->round==2){
			p = head->next->next->wq;
			if(p){
				for(; p ->next!= NULL; p = p->next);
				p->next= current;
			}
			else head->next->next->wq=current;
			//printf("fffffffffffffffff222\n");
		}else{
			p = head->next->next->wq;
			if(p){
				for(; p ->next!= NULL; p = p->next);
				p->next= current;
			}
			else head->next->next->wq=current;
			//printf("fffffffffffffffff333\n");
		}
		current =  next;
		next = NULL;
		current->job->state = RUNNING;
		current->job->wait_time = 0;
		kill(current->job->pid,SIGCONT);
		return;
	}else{ /* next == NULL且current != NULL，不切换 */
		current->round=add_round(current->round);
		//printf("continue\n");
		return;
	}
}

void sig_handler(int sig,siginfo_t *info,void *notused)
{
	int status;
	int ret;
	switch (sig) {
case SIGVTALRM: /* 到达计时器所设置的计时间隔 */
	{
	i++;
	//printf("i=====%d\n",i);
	scheduler();
	}
#ifdef DEBUG
	printf("SIGVTALRM RECEVIRD!\n");
#endif
	return;
case SIGCHLD: /* 子进程结束时传送给父进程的信号 */

#ifdef DEBUG
	tell_after();
#endif
	ret = waitpid(-1,&status,WNOHANG);
	if (ret == 0)
		return;
	if(WIFEXITED(status)){
		current->job->state = DONE;
		printf("normal termation, exit status = %d\n",WEXITSTATUS(status));
	}else if (WIFSIGNALED(status)){
		printf("abnormal termation, signal number = %d\n",WTERMSIG(status));
	}else if (WIFSTOPPED(status)){
		printf("child stopped, signal number = %d\n",WSTOPSIG(status));
	}
	return;
	default:
		return;
	}
}

void do_enq(struct jobinfo *newjob,struct jobcmd enqcmd)
{
	struct waitqueue *newnode;
	struct waitqueue *p;
	int i=0,pid;
	char *offset,*argvec,*q;
	char **arglist;
	sigset_t zeromask;

	sigemptyset(&zeromask);

	/* 封装jobinfo数据结构 */
	newjob = (struct jobinfo *)malloc(sizeof(struct jobinfo));
	newjob->jid = allocjid();
	newjob->defpri = enqcmd.defpri;
	newjob->curpri = enqcmd.defpri;
	newjob->ownerid = enqcmd.owner;
	newjob->state = READY;
	newjob->create_time = time(NULL);
	newjob->wait_time = 0;
	newjob->run_time = 0;
	arglist = (char**)malloc(sizeof(char*)*(enqcmd.argnum+1));
	newjob->cmdarg = arglist;
	offset = enqcmd.data;
	argvec = enqcmd.data;
	while (i < enqcmd.argnum){
		if(*offset == ':'){
			*offset++ = '\0';
			q = (char*)malloc(offset - argvec);
			strcpy(q,argvec);
			arglist[i++] = q;
			argvec = offset;
		}else
			offset++;
	}

	arglist[i] = NULL;

#ifdef DEBUG

	printf("enqcmd argnum %d\n",enqcmd.argnum);
	for(i = 0;i < enqcmd.argnum; i++)
		printf("parse enqcmd:%s\n",arglist[i]);

#endif

	/*向等待队列中增加新的作业*/
	newnode = (struct waitqueue*)malloc(sizeof(struct waitqueue));
	newnode->next =NULL;
	newnode->job=newjob;
	if(head->wq){
		for(p=head->wq;p->next!=NULL;p=p->next) ;
		p->next=newnode;	
	}
	else head->wq=newnode;

	if(current!=NULL){
			kill(current->job->pid,SIGSTOP);
			current->job->curpri = current->job->defpri;
			current->job->wait_time = 0;
			current->job->state = READY;
			current->next=NULL;
			if(current->round==1){
				p = head->wq;
				if(p){
					for(; p ->next!= NULL; p = p->next);
					p->next= current;
				}
				else head->next->wq=current;
				//printf("ffffffff~~~~~fffffffff111\n");
			}else if(current->round==2){
				p = head->next->wq;
				if(p){
					for(; p ->next!= NULL; p = p->next);
					p->next= current;
				}
				else head->next->next->wq=current;
				//printf("fffffffff~~~ffffffff222\n");
			}else{
				p = head->next->next->wq;
				if(p){
					for(; p ->next!= NULL; p = p->next);
					p->next= current;
				}
				else head->next->next->wq=current;

				//printf("ffffffff~~~~~~~~fffffffff333\n");
			}
		}
	current =  NULL;
	//printf("current!=NULL\n");
	/*为作业创建进程*/
	if((pid=fork())<0)
		error_sys("enq fork failed");
	if(pid==0){
		newjob->pid =getpid();
		//printf("the newjob->pid is****************:%d\n",newjob->pid);
		/*阻塞子进程,等等执行*/	 
		raise(SIGSTOP);
//#ifdef DEBUG
		printf("begin running\n");
		for(i=0;arglist[i]!=NULL;i++)
			printf("arglist %s\n",arglist[i]);
//#endif
		/*复制文件描述符到标准输出*/
		dup2(globalfd,1);
		/* 执行命令 */
		if(execv(arglist[0],arglist)<0)
			printf("exec failed\n");
		exit(1);
	}else{
		newjob->pid=pid;
		wait(NULL);
		//printf("the pid is****************:%d\n",pid);
	}
}

void do_deq(struct jobcmd deqcmd)
{
	int deqid,i;
	struct waitqueue *f=NULL,*prev_f=NULL,*select,*selectprev;
	struct queue *p,*prev;
	deqid=atoi(deqcmd.data);
	//printf("deq jid !!!!!!!!!!!!!!!%d\n",deqid);
#ifdef DEBUG
	printf("deq jid %d\n",deqid);
#endif

	/*current jodid==deqid,终止当前作业*/
	if (current && current->job->jid ==deqid){
		printf("teminate current job\n");
		kill(current->job->pid,SIGKILL);
		for(i=0;(current->job->cmdarg)[i]!=NULL;i++){
			free((current->job->cmdarg)[i]);
			(current->job->cmdarg)[i]=NULL;
		}
		free(current->job->cmdarg);
		free(current->job);
		free(current);
		current=NULL;
	}
	else{ /* 或者在等待队列中查找deqid */
		select=NULL;
		selectprev=NULL;
		if(head){
			for(prev=head,p=head;p!=NULL;prev=p,p=p->next){
				for(prev_f=f=p->wq;f!=NULL;f=f->next)
					if(f->job->jid==deqid){
						select=f;
						selectprev=prev_f;
						break;
					}
				if(select){
					selectprev->next=select->next;
					if(select==selectprev)
						p->wq=NULL;
					break;
				}
			}
		}
		if(select){
			for(i=0;(select->job->cmdarg)[i]!=NULL;i++){
				free((select->job->cmdarg)[i]);
				(select->job->cmdarg)[i]=NULL;
			}
			free(select->job->cmdarg);
			free(select->job);
			free(select);
			select=NULL;
		}
	}
}

void do_stat(struct jobcmd statcmd)
{
	struct waitqueue *p;
	struct queue *QQ,*q;
	char timebuf[BUFLEN];
	/*
	*打印所有作业的统计信息:
	*1.作业ID
	*2.进程ID
	*3.作业所有者
	*4.作业运行时间
	*5.作业等待时间
	*6.作业创建时间
	*7.作业状态
	*/

	/* 打印信息头部 */
	printf("JOBID\tPID\tOWNER\tRUNTIME\tWAITTIME\tCREATTIME\t\tSTATE\n");
	if(current){
		strcpy(timebuf,ctime(&(current->job->create_time)));
		timebuf[strlen(timebuf)-1]='\0';
		printf("%d\t%d\t%d\t%d\t%d\t%s\t%s\n",
			current->job->jid,
			current->job->pid,
			current->job->ownerid,
			current->job->run_time,
			current->job->wait_time,
			timebuf,"RUNNING");
	}
	QQ=head;
	for(;QQ!=NULL;QQ=QQ->next){
		for(p=QQ->wq;p!=NULL;p=p->next){
			strcpy(timebuf,ctime(&(p->job->create_time)));
			timebuf[strlen(timebuf)-1]='\0';
			printf("%d\t%d\t%d\t%d\t%d\t%s\t%s\n",
				p->job->jid,
				p->job->pid,
				p->job->ownerid,
				p->job->run_time,
				p->job->wait_time,
				timebuf,
				"READY");
		}
	}
}

int main()
{
	struct timeval interval;
	struct itimerval new,old;
	struct stat statbuf;
	struct sigaction newact,oldact1,oldact2;
	#ifdef DEBUG
		printf("Debug is open!\n");
	#endif
	/*  jieshi
	struct sigaction{
  	void (*sa_handler)(int);
   	sigset_t sa_mask;
  	int sa_flag;
  	void (*sa_sigaction)(int,siginfo_t *,void *);
	};*/
		
	creat_Q();
	if(stat("/tmp/server",&statbuf)==0){
		/* 如果FIFO文件存在,删掉 */
		if(remove("/tmp/server")<0)
			error_sys("remove failed");
	}

	if(mkfifo("/tmp/server",0666)<0)
		error_sys("mkfifo failed");
	/* 在非阻塞模式下打开FIFO */
	if((fifo=open("/tmp/server",O_RDONLY|O_NONBLOCK))<0)
		error_sys("open fifo failed");

	/* 建立信号处理函数 */
	newact.sa_sigaction=sig_handler;
	sigemptyset(&newact.sa_mask);
	newact.sa_flags=SA_SIGINFO;
	sigaction(SIGCHLD,&newact,&oldact1);
	sigaction(SIGVTALRM,&newact,&oldact2);

	/* 设置时间间隔为1000毫秒 */
	interval.tv_sec=1;
	interval.tv_usec=0;

	new.it_interval=interval;
	new.it_value=interval;
	setitimer(ITIMER_VIRTUAL,&new,&old);

	while(siginfo==1);

	close(fifo);
	close(globalfd);
	return 0;
}
