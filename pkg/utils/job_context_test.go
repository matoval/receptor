package utils_test

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/ansible/receptor/pkg/utils"
)

type fields struct {
	Ctx         context.Context
	JcCancel    context.CancelFunc
	Wg          *sync.WaitGroup
	JcRunning   bool
	RunningLock *sync.Mutex
}

func setupGoodFields() fields {
	goodCtx, goodCancel := context.WithCancel(context.Background())
	goodFields := &fields{
		Ctx:         goodCtx,
		JcCancel:    goodCancel,
		Wg:          &sync.WaitGroup{},
		JcRunning:   true,
		RunningLock: &sync.Mutex{},
	}

	return *goodFields
}

func TestJobContextRunning(t *testing.T) {
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name:   "Positive",
			fields: setupGoodFields(),
			want:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mw := &utils.JobContext{
				Ctx:         tt.fields.Ctx,
				JcCancel:    tt.fields.JcCancel,
				Wg:          tt.fields.Wg,
				JcRunning:   tt.fields.JcRunning,
				RunningLock: tt.fields.RunningLock,
			}
			if got := mw.Running(); got != tt.want {
				t.Errorf("JobContext.Running() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJobContextCancel(t *testing.T) {
	tests := []struct {
		name   string
		fields fields
	}{
		{
			name:   "Positive",
			fields: setupGoodFields(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mw := &utils.JobContext{
				Ctx:         tt.fields.Ctx,
				JcCancel:    tt.fields.JcCancel,
				Wg:          tt.fields.Wg,
				JcRunning:   tt.fields.JcRunning,
				RunningLock: tt.fields.RunningLock,
			}
			mw.Cancel()
		})
	}
}

func TestJobContextValue(t *testing.T) {
	type args struct {
		key interface{}
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		want   interface{}
	}{
		{
			name:   "Positive",
			fields: setupGoodFields(),
			want:   nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mw := &utils.JobContext{
				Ctx:         tt.fields.Ctx,
				JcCancel:    tt.fields.JcCancel,
				Wg:          tt.fields.Wg,
				JcRunning:   tt.fields.JcRunning,
				RunningLock: tt.fields.RunningLock,
			}
			if got := mw.Value(tt.args.key); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("JobContext.Value() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJobContextDeadline(t *testing.T) {
	tests := []struct {
		name     string
		fields   fields
		wantTime time.Time
		wantOk   bool
	}{
		{
			name:     "Positive",
			fields:   setupGoodFields(),
			wantTime: time.Time{},
			wantOk:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mw := &utils.JobContext{
				Ctx:         tt.fields.Ctx,
				JcCancel:    tt.fields.JcCancel,
				Wg:          tt.fields.Wg,
				JcRunning:   tt.fields.JcRunning,
				RunningLock: tt.fields.RunningLock,
			}
			gotTime, gotOk := mw.Deadline()
			if !reflect.DeepEqual(gotTime, tt.wantTime) {
				t.Errorf("JobContext.Deadline() gotTime = %v, want %v", gotTime, tt.wantTime)
			}
			if gotOk != tt.wantOk {
				t.Errorf("JobContext.Deadline() gotOk = %v, want %v", gotOk, tt.wantOk)
			}
		})
	}
}

func TestJobContextErr(t *testing.T) {
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name:    "Positive",
			fields:  setupGoodFields(),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mw := &utils.JobContext{
				Ctx:         tt.fields.Ctx,
				JcCancel:    tt.fields.JcCancel,
				Wg:          tt.fields.Wg,
				JcRunning:   tt.fields.JcRunning,
				RunningLock: tt.fields.RunningLock,
			}
			if err := mw.Err(); (err != nil) != tt.wantErr {
				t.Errorf("JobContext.Err() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestJobContextWait(t *testing.T) {
	tests := []struct {
		name   string
		fields fields
	}{
		{
			name:   "Positive",
			fields: setupGoodFields(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mw := &utils.JobContext{
				Ctx:         tt.fields.Ctx,
				JcCancel:    tt.fields.JcCancel,
				Wg:          tt.fields.Wg,
				JcRunning:   tt.fields.JcRunning,
				RunningLock: tt.fields.RunningLock,
			}
			mw.Wait()
		})
	}
}
