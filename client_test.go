package natsrpc

import "testing"

func TestClientNewCallOptions(t *testing.T) {
	c := &Client{
		opt: ClientOptions{
			id: "client-default",
		},
	}

	tests := []struct {
		name string
		opt  []CallOption
		want string
	}{
		{
			name: "inherit client default id",
			want: "client-default",
		},
		{
			name: "override with call id",
			opt:  []CallOption{WithCallID("call-id")},
			want: "call-id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := c.newCallOptions(tt.opt...).id
			if got != tt.want {
				t.Fatalf("newCallOptions().id = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestClientSubject(t *testing.T) {
	c := &Client{
		opt: ClientOptions{
			namespace: "ns",
		},
	}

	tests := []struct {
		name      string
		isPublish bool
		id        string
		want      string
	}{
		{
			name:      "request without id",
			isPublish: false,
			want:      "ns.svc",
		},
		{
			name:      "request with id",
			isPublish: false,
			id:        "1",
			want:      "ns.svc.1",
		},
		{
			name:      "publish with id",
			isPublish: true,
			id:        "1",
			want:      "ns.svc.1._nr_pub",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := c.subject("svc", tt.isPublish, tt.id)
			if got != tt.want {
				t.Fatalf("subject() = %q, want %q", got, tt.want)
			}
		})
	}
}
